package templates

const RayServeAPITemplate = `
from fastapi import FastAPI, Request, HTTPException, Query
import asyncio
import uvicorn
import ray

app = FastAPI()

@app.get(\"/api/health\")
async def health_check():
    return {\"status\": \"ok\"}

@app.post(\"/api/actor-status\")
async def actor_status(request: Request):
    if request.headers.get(\"content-type\") != \"application/json\":
        raise HTTPException(status_code=400, detail=\"Request must be JSON\")

    data = await request.json()
    actor_id = data.get(\"actor_id\")

    if not actor_id:
        raise HTTPException(status_code=400, detail=\"No actor_id provided\")
    
    try:
        command = f\"ray list actors --filter 'state=ALIVE' | grep {actor_id}\"
        process = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            processed_output = ' '.join(stdout.decode().strip().split())
            actor_status = processed_output.split(' ')[3] if len(processed_output.split(' ')) >= 4 else \"Unavailable\"
            return {\"status\": \"success\", \"actor_status\": actor_status}
        else:
            return {\"status\": \"error\", \"error\": stderr.decode().strip()}

    except Exception as e:
        raise HTTPException(status_code=500, detail=\"Failed to get actor status\")

@app.post(\"/api/actor-logs\")
async def actor_logs(request: Request):
    if request.headers.get(\"content-type\") != \"application/json\":
        raise HTTPException(status_code=400, detail=\"Request must be JSON\")

    data = await request.json()
    actor_id = data.get(\"actor_id\")

    if not actor_id:
        raise HTTPException(status_code=400, detail=\"No actor_id provided\")

    try:
        command = f\"ray list actors --filter 'state=ALIVE' | grep {actor_id}\"
        process = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            processed_output = ' '.join(stdout.decode().strip().split())
            id = processed_output.split(' ')[1] if len(processed_output.split(' ')) >= 4 else \"Unavailable\"

            command_logs = f\"ray logs actor --id {id} --tail 100\"
            process_logs = await asyncio.create_subprocess_shell(
                command_logs, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout_logs, stderr_logs = await process_logs.communicate()

            if process_logs.returncode == 0:
                return {\"status\": \"success\", \"logs\": stdout_logs.decode().strip()}
            else:
                return {\"status\": \"error\", \"error\": stderr_logs.decode().strip()}

        else:
            return {\"status\": \"error\", \"error\": stderr.decode().strip()}

    except Exception as e:
        raise HTTPException(status_code=500, detail=\"Failed to get actor logs\")

@app.delete(\"/api/kill-actor\")
async def kill_actor(actor_id: str = Query(...)):
    if not actor_id:
        raise HTTPException(status_code=400, detail=\"No actor_id provided\")

    try:
        actor = ray.get_actor(name=f\"{actor_id}\", namespace=\"{{.Namespace}}\")
        ray.kill(actor)
        return {\"status\": \"success\", \"detail\": f\"{actor_id} deleted\"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f\"Failed to kill actor: {str(e)}\")

if __name__ == \"__main__\":
    uvicorn.run(app, host=\"0.0.0.0\", port=8000)
`
