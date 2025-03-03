"""Hook for interacting with Keep Alert Management System."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from typing import Literal
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from pydantic import BaseModel
from pydantic import Field
from pydantic import ValidationError
from requests.exceptions import RequestException


class KeepAlertPayload(BaseModel):
    """Pydantic model for validating Keep alert payload."""

    id: Optional[str] = Field(None, description="Unique identifier for the alert")
    name: str = Field(..., description="Human-readable name of the alert")
    status: Literal["firing", "resolved", "acknowledged", "suppressed", "pending"] = (
        "firing"
    )
    severity: Literal["critical", "high", "warning", "info", "low"] = "info"
    lastReceived: Optional[str] = Field(
        None, description="ISO timestamp of last alert occurrence"
    )
    firingStartTime: Optional[str] = Field(
        None, description="ISO timestamp when alert first fired"
    )
    environment: str = "production"
    service: Optional[str] = Field(None, description="Service or application affected")
    source: list[str] = Field(
        ["python"], description="List of sources that triggered the alert"
    )
    message: Optional[str] = Field(
        None, max_length=2000, description="Concise alert summary"
    )
    description: Optional[str] = Field(
        None, max_length=2000, description="Detailed explanation"
    )
    pushed: bool = True
    url: Optional[str] = Field(None, description="URL for additional context")
    imageUrl: Optional[str] = Field(None, description="URL for relevant image")
    labels: dict[str, Any] = Field(
        default_factory=dict, description="Key-value metadata pairs"
    )
    fingerprint: Optional[str] = Field(
        None, description="Unique identifier for alert grouping"
    )
    assignee: Optional[str] = Field(
        None, description="Responsible party for addressing"
    )
    note: Optional[str] = Field(None, description="Additional commentary or notes")


class KeepHook(HttpHook):
    """
    Airflow Hook for interacting with Keep Alert Management System.

    Facilitates sending alerts to Keep through Airflow connections.

    Args:
        keep_conn_id (str): Airflow connection ID configured with Keep credentials.
        alert_data (dict[str, Any]): Dictionary containing alert parameters (see KeepAlertPayload).

    Example usage:
    ```
    hook = KeepHook(
        keep_conn_id="keep_prod",
        alert_data={
            "name": "ServiceOutage",
            "message": "API latency exceeded thresholds",
            "service": "payment-gateway",
            "severity": "critical"
        }
    )
    hook.execute()
    ```
    """

    conn_name_attr = "keep_conn_id"
    default_conn_name = "keep_default"
    conn_type = "keep"
    hook_name = "Keep"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """
        Return field behavior for Keep connection form.

        Returns:
            dict[str, Any]: Dictionary containing field behavior.
        """
        return {
            "hidden_fields": ["port", "schema", "extra", "login"],
            "relabeling": {
                "host": "Keep Endpoint",
                "password": "Keep API Key",
            },
            "placeholders": {
                "host": "https://api.keep.dev/alerts",
                "password": "Enter API key",
            },
        }

    def __init__(
        self,
        keep_conn_id: str = "keep_default",
        alert_endpoint: str = "/alerts/event",
        alert_data: Optional[dict[str, Any]] = None,
        method: str = "POST",
        *args,
        **kwargs,
    ):
        super().__init__(http_conn_id=keep_conn_id, method=method, *args, **kwargs)
        self.alert_endpoint = alert_endpoint
        self.alert_data = alert_data or {}

        self._validate_alert_data()
        self.keep_endpoint, self.keep_api_key = self._get_keep_credentials()

    def _validate_alert_data(self) -> None:
        """Validate alert data using Pydantic model."""
        try:
            KeepAlertPayload(**self.alert_data)
        except ValidationError as e:
            raise AirflowException(f"Invalid alert payload: {str(e)}")

    def _get_keep_credentials(self) -> tuple[str, str]:
        """
        Retrieve Keep credentials from Airflow connection.

        Returns:
            tuple[str, str]: Tuple containing (endpoint, API key).

        Raises:
            AirflowException: If connection details are invalid.
        """
        try:
            conn = self.get_connection(self.http_conn_id)
            if not conn.host:
                raise ValueError("Keep endpoint (host) not configured in connection")
            if not conn.password:
                raise ValueError(
                    "Keep API key not configured in connection (password field)"
                )

            return conn.host, conn.password
        except Exception as e:
            raise AirflowException(f"Failed to retrieve Keep credentials: {str(e)}")

    def _build_request_components(self) -> tuple[dict, dict]:
        """
        Construct headers and payload for Keep API request.

        Returns:
            tuple[dict, dict]: Tuple containing (headers, payload).
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-API-KEY": self.keep_api_key,
        }

        alert_data = self.alert_data.copy()
        alert_data.setdefault("lastReceived", datetime.now().isoformat())
        if alert_data.get("status") == "firing":
            alert_data.setdefault("firingStartTime", datetime.now().isoformat())

        # Validate again with updated timestamps
        payload = KeepAlertPayload(**alert_data).model_dump()

        return headers, payload

    def execute(self) -> None:
        """
        Execute the request to send alert to Keep.

        Raises:
            AirflowException: If API request fails.
        """
        headers, payload = self._build_request_components()

        try:
            response = self.run(
                endpoint=self.alert_endpoint,
                json=payload,
                headers=headers,
                extra_options={"check_response": False},
            )

            if 400 <= response.status_code < 600:
                error_msg = (
                    f"Keep API request failed: {response.status_code} - {response.text}"
                )
                raise RequestException(error_msg)

        except Exception as e:
            raise AirflowException(f"Failed to send alert to Keep: {str(e)}")
