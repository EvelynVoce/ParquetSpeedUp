from fastapi import FastAPI, File, UploadFile, HTTPException
import io
import polars as pl
import pyarrow.parquet as pq

app = FastAPI()


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded file into a buffer
        contents = await file.read()
        buffer = io.BytesIO(contents)

        # Read the Parquet file from the buffer using Polars
        df = pl.read_parquet(buffer)

        # Process the DataFrame as needed
        # For demonstration, we will just return the number of rows and columns
        num_rows, num_cols = df.shape

        return {"filename": file.filename, "num_rows": num_rows, "num_cols": num_cols}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
