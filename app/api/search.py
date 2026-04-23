from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.crud.match_crud import get_filtered_matches
from app.db.database import get_db
from app.schemas.query_schema import QueryRequest


router = APIRouter(prefix="/search", tags=["search"])


@router.post("/")
def search_matches(request: QueryRequest, db: Session = Depends(get_db)):
	try:
        # Validate the request data
		results = get_filtered_matches(db, request)
		return {
			"status": "success",
			"count": len(results),
			"data": results,
		}
	except ValueError as e:
		raise HTTPException(status_code=400, detail=str(e)) from e
	except SQLAlchemyError as e:
		db.rollback()
		raise HTTPException(
			status_code=500,
			detail="A database error occurred while searching.",
		) from e
	except Exception as e:
		raise HTTPException(
			status_code=500,
			detail=f"An unexpected error occurred while searching: {str(e)}",
		) from e
