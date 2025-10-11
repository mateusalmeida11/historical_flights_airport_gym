import json

from historical_flights_airport_gym.utils.aws.S3 import S3, S3UploadError
from historical_flights_airport_gym.utils.build_path import path_file_raw
from historical_flights_airport_gym.utils.get_data import RequestError, get_data
from historical_flights_airport_gym.utils.transformations import (
    JsonProcessingError,
    from_str_to_json,
)

s3 = S3()


def lambda_handler(event, context):
    try:
        layer = event.get("layer")
        bucket = event.get("bucket")
        dt_voo = event.get("dt_voo")

        file_name = path_file_raw()
        key = f"{layer}/flights/{file_name}.json"

        url = "https://sas.anac.gov.br/sas/vra_api/vra/data?"
        params = {"dt_voo": dt_voo}

        response = get_data(url=url, params=params)
        jsonObj = from_str_to_json(response=response)
        jsonData = json.dumps(jsonObj, indent=4)

        response_s3 = s3.upload_file(bucket=bucket, data=jsonData, key=key)

        return {
            "status": "success",
            "bucket": bucket,
            "key": key,
            "s3_response": {
                "status_code": response_s3["ResponseMetadata"]["HTTPStatusCode"]
            },
        }

    except RequestError as e:
        return {
            "status": "error",
            "type": "APIError",
            "status_code": e.status_code or 500,
            "message": str(e),
        }

    except S3UploadError as e:
        return {
            "status": "error",
            "type": "S3UploadError",
            "status_code": e.status_code,
            "message": e.message,
            "bucket": bucket,
            "key": key,
        }
    except JsonProcessingError as e:
        return {
            "status": "error",
            "type": "JSONProcessingError",
            "status_code": e.status_code,
            "message": str(e),
        }
    except Exception as e:
        return {
            "status": "error",
            "type": "LambdaError",
            "status_code": 500,
            "message": f"Erro Inesperado: {str(e)}",
        }
