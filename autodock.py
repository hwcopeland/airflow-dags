from airflow import DAG, XComArg
from airflow.decorators import dag, task, task_group
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.configuration import conf
from kubernetes.client import models as k8s
from datetime import datetime

IMAGE_NAME = 'hwcopeland/autodock-all:latest'
PVC_NAME = 'pvc-autodock'
MOUNT_PATH_AUTODOCK = '/data'
VOLUME_KEY_AUTODOCK = 'volume-autodock'

params = {
    'pdbid': '8DZ2',
    'ligand_db': 'ChEBI_complete',
    'jupyter_user': 'jovyan',
    'ligands_chunk_size': 100000,
}

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

@dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False, params=params)
def autodock():
    import os.path

    volume_autodock = k8s.V1Volume(
        name=VOLUME_KEY_AUTODOCK,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )
    volume_mount_autodock = k8s.V1VolumeMount(mount_path=MOUNT_PATH_AUTODOCK, name=VOLUME_KEY_AUTODOCK)

    jupyter_user_pvc = f"claim-{params['jupyter_user']}"
    VOLUME_KEY_USER = f"volume-user-{params['jupyter_user']}"
    MOUNT_PATH_USER = f"/home/{params['jupyter_user']}"

    # Mount the Jupyter user's PVC (from the same namespace)
    volume_user = k8s.V1Volume(
        name=VOLUME_KEY_USER,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=jupyter_user_pvc),  # Remove claim_namespace
    )
    volume_mount_user = k8s.V1VolumeMount(mount_path=MOUNT_PATH_USER, name=VOLUME_KEY_USER)

    container = k8s.V1Container(
        name='autodock-container',
        image=IMAGE_NAME,
        working_dir=MOUNT_PATH_AUTODOCK,
        volume_mounts=[volume_mount_autodock, volume_mount_user],
        image_pull_policy='Always',
    )

    pod_spec = k8s.V1PodSpec(containers=[container], volumes=[volume_autodock, volume_user])
    full_pod_spec = k8s.V1Pod(spec=pod_spec)

    # Copy ligand_db from user's PVC to the autodock PVC
    copy_ligand_db = KubernetesPodOperator(
        task_id='copy_ligand_db',
        image='alpine',
        cmds=['/bin/sh', '-c'],
        arguments=[
            f'cp {MOUNT_PATH_USER}/{{{{ params.ligand_db }}}}.sdf {MOUNT_PATH_AUTODOCK}/{{{{ params.ligand_db }}}}.sdf'
        ],
        name='copy-ligand-db',
        volumes=[volume_autodock, volume_user],
        volume_mounts=[volume_mount_autodock, volume_mount_user],
        namespace=namespace,  # Ensure this pod runs in the correct namespace
        get_logs=True,
    )

    prepare_receptor = KubernetesPodOperator(
        task_id='prepare_receptor',
        full_pod_spec=full_pod_spec,
        cmds=['/autodock/scripts/1a_fetch_prepare_protein.sh', '{{ params.pdbid }}'],
    )

    split_sdf = KubernetesPodOperator(
        task_id='split_sdf',
        full_pod_spec=full_pod_spec,
        cmds=['/bin/sh', '-c'],
        arguments=['/autodock/scripts/split_sdf.sh {{ params.ligands_chunk_size }} {{ params.ligand_db }} > /airflow/xcom/return.json'],
        do_xcom_push=True,
    )

    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=full_pod_spec,
        cmds=['/autodock/scripts/3_post_processing.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
    )

    @task
    def get_batch_labels(db_label: str, n: int):
        return [f'{db_label}_batch{i}' for i in range(n + 1)]

    @task_group
    def docking(batch_label: str):
        prepare_ligands = KubernetesPodOperator(
            task_id='prepare_ligands',
            full_pod_spec=full_pod_spec,
            get_logs=True,
            cmds=['/autodock/scripts/1b_prepare_ligands.sh'],
            arguments=['{{ params.pdbid }}', batch_label],
        )

        perform_docking = KubernetesPodOperator(
            task_id='perform_docking',
            full_pod_spec=full_pod_spec,
            cmds=['/autodock/scripts/2_docking.sh'],
            arguments=['{{ params.pdbid }}', batch_label],
            get_logs=True,
        )

        [prepare_ligands] >> perform_docking

    copy_ligand_db >> prepare_receptor >> split_sdf

    batch_labels = get_batch_labels('{{ params.ligand_db }}', split_sdf.output)
    docking_tasks = docking.expand(batch_label=batch_labels)

    docking_tasks >> postprocessing


autodock()
