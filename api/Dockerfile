FROM python:3.13-slim

WORKDIR /app

# Install uv and system dependencies
RUN pip install --no-cache-dir uv

# Copy requirements and source code
COPY api/requirements.txt ./
RUN uv pip install --system -r requirements.txt
COPY api . 
COPY shared ./shared

# Expose port
EXPOSE 4000

# Start FastAPI app with uvicorn
CMD ["uvicorn", "SERVER:app", "--host", "0.0.0.0", "--port", "4000"]