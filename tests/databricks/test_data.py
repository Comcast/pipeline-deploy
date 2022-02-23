"""
Copyright 2022 Comcast Cable Communications Management, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
"""

LIST_JOBS_NOT_OWNED = {
    "jobs": [
        {
            "creator_user_name": "not_owner@company.com",
            "job_id": '1',
            "settings": {
                "name": "Job 1"
            }
        },
        {
            "creator_user_name": "not_owner@company.com",
            "job_id": '2',
            "settings": {
                "name": "Job 2"
            }
        },
        {
            "creator_user_name": "not_owner@company.com",
            "job_id": '3',
            "settings": {
                "name": "Job 3"
            }
        },
    ]
}
LIST_JOBS_OWNED = {
    "jobs": [
        {
            "creator_user_name": "owner@company.com",
            "job_id": '1',
            "settings": {
                "name": "Job 1"
            }
        },
        {
            "creator_user_name": "owner@company.com",
            "job_id": '2',
            "settings": {
                "name": "Job 2"
            }
        },
        {
            "creator_user_name": "not_owner@company.com",
            "job_id": '3',
            "settings": {
                "name": "Job 3"
            }
        },
    ]
}
LIST_STREAMING_JOBS_OWNED = {
    "jobs": [
        {
            "creator_user_name": "owner@company.com",
            "job_id": '1',
            "settings": {
                "name": "Job 1",
                "max_retries": -1
            }
        },
        {
            "creator_user_name": "owner@company.com",
            "job_id": '2',
            "settings": {
                "name": "Job 2",
                "max_retries": -1
            }
        },
        {
            "creator_user_name": "not_owner@company.com",
            "job_id": '3',
            "settings": {
                "name": "Job 3",
                "max_retries": -1
            }
        },
    ]
}


LIST_RUNS_JOB_NOT_RUNNING = {
    "runs": [
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631101020378,
            "setup_duration": 136000,
            "execution_duration": 507000,
            "cleanup_duration": 2000,
            "end_time": 1631101666277,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/20",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "SKIPPED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631023687220,
            "setup_duration": 272000,
            "execution_duration": 409000,
            "cleanup_duration": 2000,
            "end_time": 1631024370743,
            "trigger": "ONE_TIME",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/19",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631014620513,
            "setup_duration": 186000,
            "execution_duration": 472000,
            "cleanup_duration": 2000,
            "end_time": 1631015280639,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/18",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1630928220182,
            "setup_duration": 142000,
            "execution_duration": 489000,
            "cleanup_duration": 2000,
            "end_time": 1630928853772,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/17",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        }
    ],
}
LIST_RUNS_JOB_STOPPING = {
    "runs": [
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATING",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631101020378,
            "setup_duration": 136000,
            "execution_duration": 507000,
            "cleanup_duration": 2000,
            "end_time": 1631101666277,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/20",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "SKIPPED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631023687220,
            "setup_duration": 272000,
            "execution_duration": 409000,
            "cleanup_duration": 2000,
            "end_time": 1631024370743,
            "trigger": "ONE_TIME",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/19",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631014620513,
            "setup_duration": 186000,
            "execution_duration": 472000,
            "cleanup_duration": 2000,
            "end_time": 1631015280639,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/18",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1630928220182,
            "setup_duration": 142000,
            "execution_duration": 489000,
            "cleanup_duration": 2000,
            "end_time": 1630928853772,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/17",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        }
    ],
}
LIST_RUNS_JOB_RUNNING = {
    "runs": [
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "RUNNING",
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631101020378,
            "setup_duration": 136000,
            "execution_duration": 0,
            "cleanup_duration": 2000,
            "end_time": 0,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/20",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "SKIPPED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631023687220,
            "setup_duration": 272000,
            "execution_duration": 409000,
            "cleanup_duration": 2000,
            "end_time": 1631024370743,
            "trigger": "ONE_TIME",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/19",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631014620513,
            "setup_duration": 186000,
            "execution_duration": 472000,
            "cleanup_duration": 2000,
            "end_time": 1631015280639,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/18",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1630928220182,
            "setup_duration": 142000,
            "execution_duration": 489000,
            "cleanup_duration": 2000,
            "end_time": 1630928853772,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/17",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        }
    ],
}
LIST_RUNS_JOB_PENDING = {
    "runs": [
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "PENDING",
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631101020378,
            "setup_duration": 136000,
            "execution_duration": 0,
            "cleanup_duration": 2000,
            "end_time": 0,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/20",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "SKIPPED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631023687220,
            "setup_duration": 272000,
            "execution_duration": 409000,
            "cleanup_duration": 2000,
            "end_time": 1631024370743,
            "trigger": "ONE_TIME",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/19",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1631014620513,
            "setup_duration": 186000,
            "execution_duration": 472000,
            "cleanup_duration": 2000,
            "end_time": 1631015280639,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/18",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        },
        {
            "job_id": '123456',
            "run_id": '1234567',
            "number_in_job": 20,
            "original_attempt_run_id": 123,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
                "user_cancelled_or_timedout": False
            },
            "schedule": {
                "quartz_cron_expression": "0 37 7 * * ?",
                "timezone_id": "US/Eastern",
                "pause_status": "UNPAUSED"
            },
            "task": {
                "notebook_task": {
                    "notebook_path": "/path/folder/notebook"
                }
            },
            "cluster_spec": {
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "8.2.x-scala2.12",
                    "aws_attributes": {
                        "zone_id": "us-east-1a",
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            },
            "cluster_instance": {
                "cluster_id": "123-456789-abc123",
                "spark_context_id": "12345678902345678"
            },
            "start_time": 1630928220182,
            "setup_duration": 142000,
            "execution_duration": 489000,
            "cleanup_duration": 2000,
            "end_time": 1630928853772,
            "trigger": "PERIODIC",
            "creator_user_name": "owner@company.com",
            "run_name": "job-run",
            "run_page_url": "https://databricks.com#job/123456/run/17",
            "run_type": "JOB_RUN",
            "attempt_number": 0,
            "format": "SINGLE_TASK"
        }
    ],
}

EXPORT_WORKSPACE_CHANGED_NOTEBOOK = """def fn():
    print('Hello new world!')

if __name__ == 'main':
    fn()"""
EXPORT_WORKSPACE_CHANGED_NOTEBOOK_ONLY_WHITESPACES = """def fn():
    print('Hello world!')


if __name__ == 'main':
    fn()"""
EXPORT_WORKSPACE_UNCHANGED_NOTEBOOK = """def fn():
    print('Hello world!')

if __name__ == 'main':
    fn()"""

JOBS_DATA_LOCAL = [
    {
        "name": "job-1",
        "new_cluster": {
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            },
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "ebs_volume_count": 3,
                "ebs_volume_size": 100,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "first_on_demand": 1,
                "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                "zone_id": "us-east-1a"
            },
            "enable_elastic_disk": False,
            "node_type_id": "i3.xlarge",
            "spark_conf": {
                "spark.databricks.hive.metastore.glueCatalog.enabled": "true"
            },
            "spark_version": "8.3.x-scala2.12"
        },
        "email_notifications": {
            "on_failure": [
                "name@company.com"
            ]
        },
        "schedule": {
            "pause_status": "UNPAUSED",
            "quartz_cron_expression": "0 40 * ? * * *",
            "timezone_id": "America/New_York"
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "max_retries": 3,
        "min_retry_interval_millis": 0,
        "retry_on_timeout": False,
        "format": "SINGLE_TASK",
        "notebook_task": {
            "notebook_path": "/path/notebooks/job-1",
            "base_parameters": {
                "foo": "bar"
            }
        }
    },
    {
        "name": "streaming-job-1",
        "new_cluster": {
            "num_workers": 5,
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "ebs_volume_count": 3,
                "ebs_volume_size": 100,
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "first_on_demand": 1,
                "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                "zone_id": "us-east-1a"
            },
            "enable_elastic_disk": False,
            "node_type_id": "i3.xlarge",
            "spark_conf": {
                "spark.databricks.hive.metastore.glueCatalog.enabled": "true"
            },
            "spark_version": "8.3.x-scala2.12"
        },
        "email_notifications": {
            "on_failure": [
                "name@company.com"
            ]
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "max_retries": -1,
        "min_retry_interval_millis": 0,
        "retry_on_timeout": False,
        "format": "SINGLE_TASK",
        "notebook_task": {
            "notebook_path": "/path/notebooks/streaming-job-1",
            "base_parameters": {
                "foo": "bar"
            }
        }
    }
]

JOBS_DATA_REMOTE = [
    {
        "job_id": '1',
        "settings": {
            "name": "job-1",
            "new_cluster": {
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 5
                },
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "ebs_volume_count": 3,
                    "ebs_volume_size": 100,
                    "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                    "first_on_demand": 1,
                    "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                    "zone_id": "us-east-1a"
                },
                "enable_elastic_disk": False,
                "node_type_id": "i3.xlarge",
                "spark_conf": {
                    "spark.databricks.hive.metastore.glueCatalog.enabled": "true"
                },
                "spark_version": "8.3.x-scala2.12"
            },
            "email_notifications": {
                "on_failure": [
                    "name@company.com"
                ]
            },
            "schedule": {
                "pause_status": "UNPAUSED",
                "quartz_cron_expression": "0 40 * ? * * *",
                "timezone_id": "America/New_York"
            },
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "max_retries": 3,
            "min_retry_interval_millis": 0,
            "retry_on_timeout": False,
            "format": "SINGLE_TASK",
            "notebook_task": {
                "notebook_path": "/path/notebooks/job-1",
                "base_parameters": {
                    "foo": "bar"
                }
            }
        },
        "created_time": 0,
        "creator_user_name": "name@company.com",
        "run_as_user_name": "name@company.com"
    },
    {
        "job_id": '2',
        "settings": {
            "name": "streaming-job-1",
            "new_cluster": {
                "num_workers": 5,
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "ebs_volume_count": 3,
                    "ebs_volume_size": 100,
                    "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                    "first_on_demand": 1,
                    "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                    "zone_id": "us-east-1a"
                },
                "enable_elastic_disk": False,
                "node_type_id": "i3.xlarge",
                "spark_conf": {
                    "spark.databricks.hive.metastore.glueCatalog.enabled": "true"
                },
                "spark_version": "8.3.x-scala2.12"
            },
            "email_notifications": {
                "on_failure": [
                    "name@company.com"
                ]
            },
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "max_retries": -1,
            "min_retry_interval_millis": 0,
            "retry_on_timeout": False,
            "format": "SINGLE_TASK",
            "notebook_task": {
                "notebook_path": "/path/notebooks/streaming-job-1",
                "base_parameters": {
                    "foo": "bar"
                }
            }
        },
        "created_time": 0,
        "creator_user_name": "name@company.com",
        "run_as_user_name": "name@company.com"
    }
]

JOBS_DATA_REMOTE_STREAMING = [
    {
        "job_id": '2',
        "settings": {
            "name": "streaming-job-1",
            "new_cluster": {
                "num_workers": 5,
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "ebs_volume_count": 3,
                    "ebs_volume_size": 100,
                    "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                    "first_on_demand": 1,
                    "instance_profile_arn": "arn:aws:iam:::instance-profile/path/role",
                    "zone_id": "us-east-1a"
                },
                "enable_elastic_disk": False,
                "node_type_id": "i3.xlarge",
                "spark_conf": {
                    "spark.databricks.hive.metastore.glueCatalog.enabled": "true"
                },
                "spark_version": "8.3.x-scala2.12"
            },
            "email_notifications": {
                "on_failure": [
                    "name@company.com"
                ]
            },
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "max_retries": -1,
            "min_retry_interval_millis": 0,
            "retry_on_timeout": False,
            "format": "SINGLE_TASK",
            "notebook_task": {
                "notebook_path": "/path/notebooks/streaming-job-1",
                "base_parameters": {
                    "foo": "bar"
                }
            }
        },
        "created_time": 0,
        "creator_user_name": "name@company.com",
        "run_as_user_name": "name@company.com"
    }
]

PERMISSIONS_WITH_ONLY_OWNER_AS_MANAGER = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "user_name": "owner@company.com",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                },
                {
                    "permission_level": "CAN_MANAGE",
                    "inherited": False,
                }
            ]
        }
    ]
}
PERMISSIONS_WITH_OWNER_AS_MANAGER_AND_GROUP_WITH_MANAGER_PERMISSION = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "user_name": "owner@company.com",
            "all_permissions": [
                {
                    "permission_level": "CAN_MANAGE",
                    "inherited": False,
                },
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                }
            ]
        },
        {
            "group_name": "group",
            "all_permissions": [
                {
                    "permission_level": "CAN_MANAGE",
                    "inherited": False
                }
            ]
        }
    ]
}
PERMISSIONS_WITH_OWNER_AS_MANAGER_AND_GROUP_WITH_VIEW_PERMISSION = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "user_name": "owner@company.com",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                },
                {
                    "permission_level": "CAN_MANAGE",
                    "inherited": False,
                }
            ]
        },
        {
            "group_name": "group",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": False
                }
            ]
        }
    ]
}
PERMISSIONS_WITH_OWNER = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "user_name": "owner@company.com",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                },
                {
                    "permission_level": "IS_OWNER",
                    "inherited": False,
                }
            ]
        }
    ]
}
PERMISSIONS_WITHOUT_OWNER = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "user_name": "owner@company.com",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                }
            ]
        }
    ]
}
PERMISSIONS_WITH_OWNER_GROUP = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "group_name": "group",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                },
                {
                    "permission_level": "IS_OWNER",
                    "inherited": False,
                }
            ]
        }
    ]
}
PERMISSIONS_WITH_OWNER_SERVICE_PRINCIPAL = {
    "object_id": "/jobs/123456",
    "object_type": "job",
    "access_control_list": [
        {
            "service_principal_name": "service",
            "all_permissions": [
                {
                    "permission_level": "CAN_VIEW",
                    "inherited": True,
                    "inherited_from_object": "/jobs/"
                },
                {
                    "permission_level": "IS_OWNER",
                    "inherited": False,
                }
            ]
        }
    ]
}