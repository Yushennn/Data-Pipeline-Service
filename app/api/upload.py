import os
import shutil
import tempfile
from typing import Annotated, List

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.db.models import Athlete, Match, MatchPerformance, SetPerformance
from app.services.data_processor import DataProcessor


router = APIRouter(tags=["upload"])


@router.get("/")
def read_root():
	return {"message": "Welcome to the Data Processing Pipeline API. Use /upload-match-data to upload your datasets."}


@router.post(
	"/upload-match-data",
	openapi_extra={
		"requestBody": {
			"content": {
				"multipart/form-data": {
					"schema": {
						"type": "object",
						"properties": {
							"files": {
								"type": "array",
								"items": {"type": "string", "format": "binary"},
							}
						},
						"required": ["files"],
					}
				}
			},
			"required": True,
		}
	},
)
async def upload_match_data(files: Annotated[List[UploadFile], File(...)], db: Session = Depends(get_db)):
	if not files:
		raise HTTPException(status_code=400, detail="請至少上傳一個檔案")

	for file in files:
		if not file.filename.endswith((".xlsx", ".xls")):
			raise HTTPException(status_code=400, detail="Only Excel files (.xlsx or .xls) are allowed.")

	with tempfile.TemporaryDirectory() as tmpdir:
		for file in files:
			file_path = os.path.join(tmpdir, file.filename)

			with open(file_path, "wb") as buffer:
				shutil.copyfileobj(file.file, buffer)

		try:
			print(f"Have received {len(files)} files")
			processor = DataProcessor(folder_path=tmpdir)
			athletes_df, matches_df, sets_df, match_totals_df = processor.readXlsx()
			athletes_data = athletes_df.to_dict(orient="records")
			matches_data = matches_df.to_dict(orient="records")
			sets_data = sets_df.to_dict(orient="records")
			match_totals_data = match_totals_df.to_dict(orient="records")

			print("Is writing to postgreSQL")
			if athletes_data:
				stmt = insert(Athlete).values(athletes_data)
				stmt = stmt.on_conflict_do_nothing(index_elements=["athlete_id"])
				db.execute(stmt)

			if matches_data:
				stmt = insert(Match).values(matches_data)
				stmt = stmt.on_conflict_do_nothing(index_elements=["match_id"])
				db.execute(stmt)

			if match_totals_data:
				stmt = insert(MatchPerformance).values(match_totals_data)
				stmt = stmt.on_conflict_do_nothing(index_elements=["match_performance_id"])
				db.execute(stmt)

			if sets_data:
				stmt = insert(SetPerformance).values(sets_data)
				stmt = stmt.on_conflict_do_nothing(index_elements=["set_performance_id"])
				db.execute(stmt)

			db.commit()

			print("Data inserted successfully into the database.")

			return {
				"status": "success",
				"message": "Files received and database updated successfully.",
				"record_inserted": {
					"athletes": len(athletes_data),
					"matches": len(matches_data),
					"sets": len(sets_data),
					"match_totals": len(match_totals_data),
				},
			}
		except Exception as e:
			print(f"Error processing file: {e}")
			print("finished database operation with errors")
			db.rollback()
			raise HTTPException(
				status_code=500,
				detail=f"An error occurred while processing the file: {str(e)}",
			) from e
