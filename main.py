from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional, List
import uuid
from datetime import date

app = FastAPI()

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (update for production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "project-1d951180-a0ea-4a20-bc3"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------
class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = 0.0

class PropertyUpdate(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    property_type: Optional[str] = None
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None

class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: str

class ExpenseCreate(BaseModel):
    amount: float
    date: date
    description: str

# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property(property_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns a single property by ID."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = @property_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "STRING", property_id)]
    )
    
    results = list(bq.query(query, job_config=job_config).result())
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return dict(results[0])

@app.get("/income/{property_id}")
def get_property_income(property_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all income records for a specific property."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = @property_id ORDER BY date DESC"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "STRING", property_id)]
    )
    results = bq.query(query, job_config=job_config).result()
    return [dict(row) for row in results]

@app.post("/income/{property_id}", status_code=status.HTTP_201_CREATED)
def create_income(property_id: str, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Creates a new income record for a property."""
    income_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (income_id, property_id, amount, date, description)
        VALUES (@income_id, @property_id, @amount, @date, @description)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("income_id", "STRING", income_id),
            bigquery.ScalarQueryParameter("property_id", "STRING", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", income.amount),
            bigquery.ScalarQueryParameter("date", "DATE", income.date),
            bigquery.ScalarQueryParameter("description", "STRING", income.description),
        ]
    )
    try:
        bq.query(query, job_config=job_config).result()
        return {"message": "Income created successfully", "income_id": income_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create income: {str(e)}")

@app.get("/expenses/{property_id}")
def get_property_expenses(property_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all expense records for a specific property."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = @property_id ORDER BY date DESC"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "STRING", property_id)]
    )
    results = bq.query(query, job_config=job_config).result()
    return [dict(row) for row in results]

@app.post("/expenses/{property_id}", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: str, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Creates a new expense record for a property."""
    expense_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (expense_id, property_id, amount, date, description)
        VALUES (@expense_id, @property_id, @amount, @date, @description)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expense_id", "STRING", expense_id),
            bigquery.ScalarQueryParameter("property_id", "STRING", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", expense.amount),
            bigquery.ScalarQueryParameter("date", "DATE", expense.date),
            bigquery.ScalarQueryParameter("description", "STRING", expense.description),
        ]
    )
    try:
        bq.query(query, job_config=job_config).result()
        return {"message": "Expense created successfully", "expense_id": expense_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create expense: {str(e)}")

# ---------------------------------------------------------------------------
# Additional Endpoints
# ---------------------------------------------------------------------------

@app.post("/properties", status_code=201)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    property_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties` 
        (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
        VALUES (@id, @name, @addr, @city, @st, @zip, @type, @tenant, @rent)
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", property_id),
        bigquery.ScalarQueryParameter("name", "STRING", prop.name),
        bigquery.ScalarQueryParameter("addr", "STRING", prop.address),
        bigquery.ScalarQueryParameter("city", "STRING", prop.city),
        bigquery.ScalarQueryParameter("st", "STRING", prop.state),
        bigquery.ScalarQueryParameter("zip", "STRING", prop.postal_code),
        bigquery.ScalarQueryParameter("type", "STRING", prop.property_type),
        bigquery.ScalarQueryParameter("tenant", "STRING", prop.tenant_name),
        bigquery.ScalarQueryParameter("rent", "FLOAT64", prop.monthly_rent),
    ]
    bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return {"property_id": property_id}

@app.put("/properties/{property_id}")
def update_property(property_id: str, prop: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    # Build dynamic UPDATE query based on provided fields
    update_data = prop.dict(exclude_unset=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields provided for update")
    
    set_clause = ", ".join([f"{k} = @{k}" for k in update_data.keys()])
    query = f"UPDATE `{PROJECT_ID}.{DATASET}.properties` SET {set_clause} WHERE property_id = @id"
    
    params = [bigquery.ScalarQueryParameter(k, "STRING" if isinstance(v, str) else "FLOAT64", v) for k, v in update_data.items()]
    params.append(bigquery.ScalarQueryParameter("id", "STRING", property_id))
    
    bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return {"message": "Property updated successfully"}

@app.delete("/properties/{property_id}")
def delete_property(property_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = @id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", property_id)])
    bq.query(query, job_config=job_config).result()
    return {"message": "Property deleted successfully"}

@app.delete("/income/{income_id}")
def delete_income(income_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE income_id = @id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", income_id)])
    bq.query(query, job_config=job_config).result()
    return {"message": "Income record deleted successfully"}

@app.delete("/expenses/{expense_id}")
def delete_expense(expense_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE expense_id = @id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", expense_id)])
    bq.query(query, job_config=job_config).result()
    return {"message": "Expense record deleted successfully"}