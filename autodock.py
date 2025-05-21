from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.configuration import conf
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
# from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import add_xcom_sidecar # Not directly used in this modified DAG for add_xcom_sidecar

IMAGE_NAME = 'hwcopeland/autodocker-vina:latest'
PVC_NAME = 'pvc-autodock'
MOUNT_PATH_AUTODOCK = '/data'
VOLUME_KEY_AUTODOCK = 'volume-autodock'

# Define the datashare volume key and mount path
VOLUME_KEY_DATASHARE = 'datashare-volume'
# The mount path for the datashare volume inside the Jupyter pod.
# Assuming 'jovyan' is the standard user in Jupyter Lab environments.
MOUNT_PATH_DATASHARE = '/home/jovyan/datashare'

params = {
    'pdbid': '6w35',
    'ligand_db': 'chemblntd_all',
    'native_ligand': 'SKV',
    'ligands_chunk_size': 1000,
}

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

@dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False, params=params)
def autodock():
    # Volume for the Autodock application's persistent storage
    volume_autodock = k8s.V1Volume(
        name=VOLUME_KEY_AUTODOCK,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )
    volume_mount_autodock = k8s.V1VolumeMount(mount_path=MOUNT_PATH_AUTODOCK, name=VOLUME_KEY_AUTODOCK)

    # Define the jupyter-datashare PVC volume and mount
    jupyter_datashare_pvc_name = "jupyter-datashare"
    volume_datashare = k8s.V1Volume(
        name=VOLUME_KEY_DATASHARE,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=jupyter_datashare_pvc_name),
    )
    volume_mount_datashare = k8s.V1VolumeMount(mount_path=MOUNT_PATH_DATASHARE, name=VOLUME_KEY_DATASHARE)

    resource_requirements = k8s.V1ResourceRequirements(
        requests={
            "cpu": "500m",  # 0.5 CPU
            "memory": "2Gi" # 2 Gigabytes of memory
        },
        limits={
            "cpu": "1",    # 1 CPU
            "memory": "4Gi" # 4 Gigabytes of memory
        }
    )
    
    # Configure the base container with both volumes mounted
    container = k8s.V1Container(
        name='autodock-container',
        image=IMAGE_NAME,
        working_dir=MOUNT_PATH_AUTODOCK,
        volume_mounts=[volume_mount_autodock, volume_mount_datashare], # Mount both volumes
        image_pull_policy='Always',
        resource_requirements,
    )

    # Configure the base pod spec with both volumes
    pod_spec = k8s.V1PodSpec(containers=[container], volumes=[volume_autodock, volume_datashare])
    full_pod_spec = k8s.V1Pod(spec=pod_spec)

    # Removed the copy_ligand_db task as the database is now directly mounted.

    prepare_receptor = KubernetesPodOperator(
        task_id='prepare_receptor',
        full_pod_spec=full_pod_spec,
        cmds=['python3','/autodock/scripts/proteinprepv2.py','--protein_id', '{{ params.pdbid }}','--ligand_id','{{ params.native_ligand }}'],
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_finish_action='delete_pod',
        namespace=namespace, # Ensure namespace is set for all PodOperators
    )

    split_sdf = KubernetesPodOperator(
        task_id='split_sdf',
        full_pod_spec=full_pod_spec,
        cmds=['/bin/sh', '-c'],
        # The ligand database is now accessed from the datashare mount path
        arguments=[f'/autodock/scripts/split_sdf.sh {{ params.ligands_chunk_size }} {MOUNT_PATH_DATASHARE}/{{{{ params.ligand_db }}}}.sdf > /airflow/xcom/return.json'],
        do_xcom_push=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_finish_action='delete_pod',
        namespace=namespace,
    )

    @task
    def get_batch_labels(batch_count: int, **context):
        ligand_db = context['params'].get('ligand_db')
        return [f"{ligand_db}_batch{i}" for i in range(batch_count + 1)]

    @task_group
    def docking(batch_label):
        prepare_ligands = KubernetesPodOperator(
            task_id='prepare_ligands',
            full_pod_spec=full_pod_spec,
            get_logs=True,
            cmds=['python3', '/autodock/scripts/ligandprepv2.py'],
            arguments=[
                # The input SDF path needs to reference the datashare mount
                f"{MOUNT_PATH_DATASHARE}/{{{{ ti.xcom_pull(task_ids='split_sdf')[ti.map_index] }}}}", # Assuming split_sdf returns actual file paths
                f"{MOUNT_PATH_AUTODOCK}/{{{{ ti.xcom_pull(task_ids='get_batch_labels')[ti.map_index] }}}}/output",
                '--format', 'pdb'
            ],
            env_vars={'MOUNT_PATH_AUTODOCK': MOUNT_PATH_AUTODOCK},
            on_finish_action='delete_pod',
            namespace=namespace,
        )
        perform_docking = KubernetesPodOperator(
            task_id='perform_docking',
            full_pod_spec=full_pod_spec,
            cmds=['/bin/sh', '-c'],
            arguments=[f'python3 /autodock/scripts/dockingv2.py {{ params.pdbid }} {{ ti.xcom_pull(task_ids="get_batch_labels")[ti.map_index] }}/output || (echo "Command failed, keeping the pod alive for debugging"; sleep 3600)'],
            get_logs=True,
            on_finish_action='delete_pod',
            namespace=namespace,
        )

        prepare_ligands >> perform_docking

    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=full_pod_spec,
        cmds=['/autodock/scripts/3_post_processing.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_finish_action='delete_pod',
        namespace=namespace,
    )

    # Task dependencies: No copy_ligand_db needed anymore
    prepare_receptor >> split_sdf

    batch_count = split_sdf.output
    batch_labels = get_batch_labels(batch_count)

    docking_tasks = docking.expand(batch_label=batch_labels)

    docking_tasks >> postprocessing

autodock()
