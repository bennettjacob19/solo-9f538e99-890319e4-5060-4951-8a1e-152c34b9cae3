FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SCHED_INVENTORY=/app/data/inventory.json

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app
COPY data /app/data

EXPOSE 8000

# 容器内以清单文件为初始库存；可通过 compose 挂载覆盖该文件
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
