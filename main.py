from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

app = FastAPI(title="Property Management API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "project-1d951180-a0ea-4a20-bc3"
DATASET = "property_mgmt"

# --- Pydantic Models for Requests ---
class IncomeCreate(BaseModel):
    income_id: int
    amount: float
    date: date
    description: Optional[str] = None

class ExpenseCreate(BaseModel):
    expense_id: int
    amount: float
    date: date
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None

class PropertyUpdate(BaseModel):
    name: Optional[str] = None
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None

class PropertyCreate(BaseModel):
    property_id: int
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float

# --- Dependency ---
def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# --- Required Endpoints ---

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` ORDER BY property_id"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return dict(results[0])

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/income/{property_id}", status_code=201)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    rows_to_insert = [{
        "income_id": income.income_id,
        "property_id": property_id,
        "amount": income.amount,
        "date": str(income.date),
        "description": income.description
    }]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.income", rows_to_insert)
    if errors:
        raise HTTPException(status_code=400, detail=f"Insert failed: {errors}")
    return {"message": "Income record created"}

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/expenses/{property_id}", status_code=201)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    rows_to_insert = [{
        "expense_id": expense.expense_id,
        "property_id": property_id,
        "amount": expense.amount,
        "date": str(expense.date),
        "category": expense.category,
        "vendor": expense.vendor,
        "description": expense.description
    }]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.expenses", rows_to_insert)
    if errors:
        raise HTTPException(status_code=400, detail=f"Insert failed: {errors}")
    return {"message": "Expense record created"}


# --- Additional Endpoints ---

@app.post("/properties", status_code=201)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Creates a brand new property record."""
    rows_to_insert = [prop.dict()]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.properties", rows_to_insert)
    if errors:
        raise HTTPException(status_code=400, detail=f"Insert failed: {errors}")
    return {"message": f"Property '{prop.name}' created successfully"}

@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Deletes a property by ID."""
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    try:
        bq.query(query).result()
        return {"message": f"Property {property_id} and associated metadata removed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tenants")
def get_tenants(bq: bigquery.Client = Depends(get_bq_client)):
    """Returns a list of all current tenants and the names of the properties they occupy."""
    query = f"""
        SELECT 
            tenant_name, 
            name as property_name 
        FROM `{PROJECT_ID}.{DATASET}.properties` 
        WHERE tenant_name IS NOT NULL
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.delete("/income/{income_id}")
def delete_income(income_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Removes a specific income record by ID."""
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE income_id = {income_id}"
    bq.query(query).result()
    return {"message": f"Income record {income_id} deleted"}

@app.delete("/expenses/{expense_id}")
def delete_expense(expense_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Removes a specific expense record by ID."""
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE expense_id = {expense_id}"
    bq.query(query).result()
    return {"message": f"Expense record {expense_id} deleted"}