from __future__ import annotations
import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    "firstMiaeDag",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="un premier DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["MIAE"],
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )

    templated_command = textwrap.dedent(
        """\
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
        """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1.doc_md = textwrap.dedent(
        """\
        #### Task Documentation
        Vous pouvez documenter votre tâche en utilisant les attributs doc_md (markdown), doc (texte brut), doc_rst,
        doc_json, doc_yaml, dont le contenu est rendu dans la page "Task Instance Details" de l'interface utilisateur.
        """
    )
    
    dag.doc_md = """ Voici une documentation qui peut être placée n'importe où."""

    t1 >> [t2, t3]
