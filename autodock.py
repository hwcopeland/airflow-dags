from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.configuration import conf
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

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

    # Define the base pod override for all tasks that will run in their own pods
    # This includes @task decorated Python functions and KubernetesPodOperators
    base_pod_override = k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base", # Important: "base" is the default container name for Airflow tasks in Kubernetes Executor
                    image=IMAGE_NAME,
                    working_dir=MOUNT_PATH_AUTODOCK,
                    volume_mounts=[volume_mount_autodock, volume_mount_datashare],
                    image_pull_policy='Always',
                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "4",
                            "memory": "16Gi"
                        },
                        limits={
                            "cpu": "4",
                            "memory": "16Gi"
                        }
                    )
                )
            ],
            # Ensure volumes are also specified at the pod level for the override
            volumes=[volume_autodock, volume_datashare]
        )
    )

    # For KubernetesPodOperator, `full_pod_spec` will still be used, but we'll include
    # the resources directly in its container definition, matching the override approach
    # to maintain consistency and clarity.

    # KubernetesPodOperator tasks will directly use the desired image and resources
    # within their full_pod_spec. The 'pod_override' is primarily for PythonOperator/BashOperator tasks.
    # However, for consistency in resource allocation, we define it here for KPO too.
    kpo_container_with_resources = k8s.V1Container(
        name='autodock-container', # This can be any name, but matches the context
        image=IMAGE_NAME,
        working_dir=MOUNT_PATH_AUTODOCK,
        volume_mounts=[volume_mount_autodock, volume_mount_datashare],
        image_pull_policy='Always',
        resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "4",
                "memory": "16Gi"
            },
            limits={
                "cpu": "4",
                "memory": "16Gi"
            }
        )
    )
    kpo_full_pod_spec = k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[kpo_container_with_resources],
            volumes=[volume_autodock, volume_datashare]
        )
    )

    # Removed the copy_ligand_db task as the database is now directly mounted.

    prepare_receptor = KubernetesPodOperator(
        task_id='prepare_receptor',
        full_pod_spec=kpo_full_pod_spec, # Use the KPO specific full_pod_spec
        cmds=['python3','/autodock/scripts/proteinprepv2.py','--protein_id', '{{ params.pdbid }}','--ligand_id','{{ params.native_ligand }}'],
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_finish_action='delete_pod',
        namespace=namespace,
        # No need for executor_config here as full_pod_spec handles everything for KPO
    )

    split_sdf = KubernetesPodOperator(
        task_id='split_sdf',
        full_pod_spec=kpo_full_pod_spec, # Use the KPO specific full_pod_spec
        cmds=['/bin/sh', '-c'],
        arguments=[f'/autodock/scripts/split_sdf.sh {{ params.ligands_chunk_size }} {MOUNT_PATH_DATASHARE}/{{{{ params.ligand_db }}}}.sdf > /airflow/xcom/return.json'],
        do_xcom_push=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_finish_action='delete_pod',
        namespace=namespace,
    )

    @task(executor_config={"pod_override": base_pod_override}) # Apply override to decorated task
    def get_batch_labels(batch_count: int, **context):
        # This task will run in a pod with the specified image and resources
        ligand_db = context['params'].get('ligand_db')
        return [f"{ligand_db}_batch{i}" for i in range(batch_count + 1)]

    @task_group
    def docking(batch_label):
        prepare_ligands = KubernetesPodOperator(
            task_id='prepare_ligands',
            full_pod_spec=kpo_full_pod_spec, # Use the KPO specific full_pod_spec
            get_logs=True,
            cmds=['python3', '/autodock/scripts/ligandprepv2.py'],
            arguments=[
                f"{MOUNT_PATH_DATASHARE}/{{{{ ti.xcom_pull(task_ids='split_sdf')[ti.map_index] }}}}",
                f"{MOUNT_PATH_AUTODOCK}/{{{{ ti.xcom_pull(task_ids='get_batch_labels')[ti.map_index] }}}}/output",
                '--format', 'pdb'
            ],
            env_vars={'MOUNT_PATH_AUTODOCK': MOUNT_PATH_AUTODOCK},
            on_finish_action='delete_pod',
            namespace=namespace,
        )
        perform_docking = KubernetesPodOperator(
            task_id='perform_docking',
            full_pod_spec=kpo_full_pod_spec, # Use the KPO specific full_pod_spec
            cmds=['/bin/sh', '-c'],
            arguments=[f'python3 /autodock/scripts/dockingv2.py {{ params.pdbid }} {{ ti.xcom_pull(task_ids="get_batch_labels")[ti.map_index] }}/output || (echo "Command failed, keeping the pod alive for debugging"; sleep 3600)'],
            get_logs=True,
            on_finish_action='delete_pod',
            namespace=namespace,
        )

        prepare_ligands >> perform_docking

    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=kpo_full_pod_spec, # Use the KPO specific full_pod_spec
        cmds=['/autodock/scripts/3_post_processing.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_finish_action='delete_pod',
        namespace=namespace,
    )

    # Task dependencies
    prepare_receptor >> split_sdf

    batch_count = split_sdf.output
    batch_labels = get_batch_labels(batch_count) # This task will use the pod_override

    docking_tasks = docking.expand(batch_label=batch_labels)

    docking_tasks >> postprocessing

autodock()
