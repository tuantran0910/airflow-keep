"""Airflow Notifier implementation for Keep alerting system."""
from __future__ import annotations

from datetime import datetime
from functools import cached_property
from typing import Any
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.notifications.basenotifier import BaseNotifier
from pydantic import ValidationError

from airflow_providers.keep.hooks.keep import KeepAlertPayload
from airflow_providers.keep.hooks.keep import KeepHook


class KeepNotifier(BaseNotifier):
    """
    Airflow Notifier for sending alerts to Keep.

    Inherits from BaseNotifier and uses KeepHook for communication.

    Args:
        keep_conn_id: Airflow connection ID configured with Keep credentials
        alert_endpoint: Keep alert endpoint URL. Default to "/alerts/event".
        alert_data: Dictionary containing alert parameters (see KeepAlertPayload)

    Example usage:
    ```python
    notifier = KeepNotifier(
        keep_conn_id="keep_prod",
        alert_data={
            "name": "TaskFailure",
            "message": "{{ task_instance.task_id }} failed",
            "service": "data-pipeline",
            "severity": "critical"
        }
    )
    ```
    """

    template_fields = [
        "keep_conn_id",
        "alert_data",
    ]

    def __init__(
        self,
        keep_conn_id: str = "keep_default",
        alert_endpoint: str = "/alerts/event",
        alert_data: Optional[dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.keep_conn_id = keep_conn_id
        self.alert_endpoint = alert_endpoint
        self.alert_data = alert_data or {}

        self._validate_alert_data()
        self._enrich_with_defaults()

    def _validate_alert_data(self) -> None:
        """
        Validate alert data using KeepAlertPayload model.
        """
        try:
            KeepAlertPayload(**self.alert_data)
        except ValidationError as e:
            raise AirflowException(f"Invalid alert data: {str(e)}")

    def _enrich_with_defaults(self) -> None:
        """
        Add default values for missing required fields.
        """
        if "lastReceived" not in self.alert_data:
            self.alert_data["lastReceived"] = datetime.now().isoformat()

        if (
            self.alert_data.get("status") == "firing"
            and "firingStartTime" not in self.alert_data
        ):
            self.alert_data["firingStartTime"] = datetime.now().isoformat()

    @cached_property
    def hook(self) -> KeepHook:
        """
        Initialize and cache KeepHook instance.
        """
        return KeepHook(
            keep_conn_id=self.keep_conn_id,
            alert_endpoint=self.alert_endpoint,
            alert_data=self.alert_data,
        )

    def _enrich_with_context(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Enrich alert data with Airflow task context.
        """
        task_instance = context.get("task_instance")
        dag_run = context.get("dag_run")

        context_data = {
            "labels": {
                "dag_id": context.get("dag").dag_id if context.get("dag") else None,
                "task_id": context.get("task").task_id if context.get("task") else None,
                "run_id": dag_run.run_id if dag_run else None,
                "execution_date": dag_run.execution_date.isoformat()
                if dag_run
                else None,
                "try_number": task_instance.try_number if task_instance else None,
            }
        }

        # Add Airflow-specific context to labels
        if "labels" not in self.alert_data:
            self.alert_data["labels"] = {}
        self.alert_data["labels"].update(context_data["labels"])

        return context_data

    def notify(self, context: Any) -> None:
        """
        Send alert notification to Keep.

        Args:
            context (Any): Airflow task context dictionary.
        Raises:
            AirflowException: If notification fails.
        """
        try:
            # Enrich alert data with task context information
            enriched_data = self._enrich_with_context(context)
            self.hook.alert_data.update(enriched_data)

            self.hook.execute()
        except Exception as e:
            raise AirflowException(f"Failed to send Keep notification: {str(e)}")
