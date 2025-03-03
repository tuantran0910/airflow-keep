def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-keep",
        "name": "Keep",
        "description": "`Keep <https://www.keephq.dev/>`__\n",
        "state": "ready",
        "dependencies": ["apache-airflow>=2.9.0"],
        "versions": [
            "1.0.0",
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow_providers.keep.hooks.keep.KeepHook",
                "connection-type": "keep",
            }
        ],
    }
