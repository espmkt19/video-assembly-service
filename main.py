from fastapi import FastAPI
from fastapi import BackgroundTasks
from pydantic import BaseModel
import os
import requests
import ffmpeg
import boto3
import uuid

app = FastAPI()

# Cloudflare R2 config
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL")

session = boto3.session.Session()
s3client = session.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

class Clip(BaseModel):
    url: str
    duration: float
    sceneNum: int

class RenderRequest(BaseModel):
    titulo: str
    videoClips: list[Clip]
    narrationUrl: str
    transitions: bool = True
    colorGrade: bool = True
    webhook: str | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


def download(url, dest):
    r = requests.get(url, stream=True)
    with open(dest, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)


def upload_to_r2(file_path, key):
    s3client.upload_file(file_path, R2_BUCKET_NAME, key)
    return f"{R2_PUBLIC_URL}/{key}"


def process_video(req: RenderRequest):
    os.makedirs("/tmp/final", exist_ok=True)

    local_videos = []
    for clip in req.videoClips:
        temp_file = f"/tmp/scene_{clip.sceneNum}.mp4"
        download(clip.url, temp_file)
        local_videos.append(temp_file)

    narration_path = "/tmp/narration.mp3"
    download(req.narrationUrl, narration_path)

    # FFmpeg concat list
    list_path = "/tmp/videos.txt"
    with open(list_path, "w") as f:
        for vid in local_videos:
            f.write(f"file '{vid}'\n")

    output_path = f"/tmp/final/{uuid.uuid4()}.mp4"

    # Concat + audio mix
    (
        ffmpeg
        .input(list_path, format="concat", safe=0)
        .output(narration_path, output_path, shortest=None, vcodec="libx264", acodec="aac")
        .run(overwrite_output=True)
    )

    # Upload final
    final_key = f"renders/{uuid.uuid4()}.mp4"
    final_url = upload_to_r2(output_path, final_key)

    # Send webhook if provided
    if req.webhook:
        requests.post(req.webhook, json={"final_url": final_url})

    return final_url


@app.post("/render")
def render(req: RenderRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_video, req)
    return {"status": "processing"}
