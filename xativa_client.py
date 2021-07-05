import click
import requests
from ast import literal_eval
import json


@click.group()
def cli():
    pass


@click.command()
@click.option("--name", default="name",
              help='Name of dataset. Default is name.')
@click.option("--source_id", default=1,
              help='Source id in configuration. Default is 1.')
@click.option("--provider_code", default=0,
              help='Provider code. Default is 0, standing for FATE.')
@click.option("--dataset", default="ds1",
              help='Dataset to be request. Default is ds1.')
def request_remote_data(name, source_id, provider_code, dataset):
    """Request remote data"""
    request = dict()
    request['name'] = name
    request['source_id'] = source_id
    request['provider_code'] = provider_code
    request['dataset'] = dataset
    request['param'] = "{}"
    response = requests.post(url="http://localhost:8080/xativa/client/v1/data/remote", json=request)
    content = json.loads(response.content)
    content['metadata'] = json.dumps(json.loads(content['metadata']))
    click.echo(json.dumps(content, indent=4))


@click.command()
@click.option("--file", default="/var/data/projects/fate/python/fate_client/data/breast_step_b.csv",
              help='File to be uploaded.\nDefault is /var/data/projects/fate/python/fate_client/data/breast_step_b.csv.')
@click.option("--partition", default=1, help='Partition to split. Default is 1.')
def upload_local_data(file, partition):
    """upload local data"""
    request = dict()
    request['file'] = file
    request['partition'] = partition
    request['head'] = 1
    request['work_mode'] = 1
    request['backend'] = 0
    request['use_local_data'] = 0
    response = requests.post(url="http://localhost:8080/xativa/client/v1/data/local", json=request)
    click.echo(json.dumps(json.loads(response.content), indent=4))


@click.command()
@click.argument("guest_table_name")
@click.argument("guest_namespace")
@click.argument("host_table_name")
@click.argument("host_namespace")
def start_job(guest_table_name, guest_namespace, host_table_name, host_namespace):
    """Start fate job."""
    request = """
    {
        "conf": {
            "initiator": {
                "role": "guest",
                "party_id": 10002
            },
            "job_parameters": {
                "work_mode": 1
            },
            "role": {
                "guest": [
                    10002
                ],
                "host": [
                    10000
                ]
            },
            "role_parameters": {
                "guest": {
                    "args": {
                        "data": {
                            "data": [
                                {
                                    "name": "guest_table_name",
                                    "namespace": "guest_namespace"
                                }
                            ]
                        }
                    },
                    "dataio_0": {
                        "with_label": [
                            false
                        ],
                        "output_format": [
                            "dense"
                        ]
                    }
                },
                "host": {
                    "args": {
                        "data": {
                            "data": [
                                {
                                    "name": "host_table_name",
                                    "namespace": "host_namespace"
                                }
                            ]
                        }
                    },
                    "dataio_0": {
                        "with_label": [
                            false
                        ],
                        "output_format": [
                            "dense"
                        ]
                    }
                }
            },
            "algorithm_parameters": {
                "intersect_0": {
                    "intersect_method": "raw",
                    "sync_intersect_ids": true,
                    "join_role": "host",
                    "with_encode": true,
                    "only_output_key": true,
                    "encode_params": {
                        "encode_method": "sha256",
                        "salt": "12345",
                        "base64": true
                    }
                }
            }
        },
            "dsl": {
            "components" : {
                "dataio_0": {
                    "module": "DataIO",
                    "input": {
                        "data": {
                            "data": [
                                "args.data"
                            ]
                        }
                    },
                    "output": {
                        "data": ["data"]
                    },
                    "need_deploy": false
                },
                "intersect_0": {
                    "module": "Intersection",
                    "input": {
                        "data": {
                            "data": [
                                "dataio_0.data"
                            ]
                        }
                    },
                    "output": {
                        "data": ["intersect_data"]
                    }
                }
            }
        }
    }"""
    request = request.replace("guest_table_name", guest_table_name) \
        .replace("guest_namespace", guest_namespace) \
        .replace("host_table_name", host_table_name) \
        .replace("host_namespace", host_namespace)

    response = requests.post(url="http://localhost:8080/xativa/client/v1/job/start",
                             json=json.loads(request))
    click.echo(json.dumps(json.loads(response.content), indent=4))


cli.add_command(upload_local_data)
cli.add_command(request_remote_data)
cli.add_command(start_job)
