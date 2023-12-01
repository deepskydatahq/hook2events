from fastapi import FastAPI, Request
from fastapi import HTTPException
from pydantic import BaseModel
import uvicorn
import dlt
from dlt.sources.helpers import requests

app = FastAPI()

class WebhookData(BaseModel):
    json_data: dict
    name: str

@app.post("/hook/{name}")
def process_webhook(name: str, request: Request):
    try:
        json_data = request.json()
        # Call your function passing the JSON and webhook name
        process_webhook_data(json_data, name)
        return {"message": "Webhook processed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def process_webhook_data(json_data: dict, name: str):
    # Your logic to process the webhook data goes here
    print(f"Received webhook data for {name}: {json_data}")

    pipeline = dlt.pipeline(
        pipeline_name=f"webhooks_pipeline",
        destination='bigquery',
        dataset_name="source_webhooks"
    )
    info = pipeline.run([json_data],table_name=f"webhook_{name}")
    print(info)



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
