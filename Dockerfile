FROM python:3.9-slim
WORKDIR /opt/render/project/src
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN chmod +x start.sh
CMD ["./start.sh"]
