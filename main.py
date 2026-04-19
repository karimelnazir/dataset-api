"""
Government Dataset API
FastAPI wrapper around the MySQL database on Aiven.
Swagger UI available at: /docs
ReDoc available at:      /redoc
"""

from datetime import date
from typing import Optional, List, Literal

import pymysql
from pymysql.cursors import DictCursor
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr, Field, field_validator


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(
    title="Government Dataset API",
    description=(
        "REST API for the Government Dataset database.\n\n"
        "Built for Milestone III — Application Layer (bonus).\n\n"
        "Use the **Try it out** button on each endpoint to test."
    ),
    version="1.0.0",
    contact={"name": "Dataset Project"},
)

# Allow the docs page and any simple frontend to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host": "mysql-108ea642-aucegypt-cff4.j.aivencloud.com",
    "port": 26453,
    "user": "avnadmin",
    "password": "AVNS_8-N4mDxKM6TOxeg_5-8",
    "database": "defaultdb",
    "ssl": {"ssl": {}},
    "cursorclass": DictCursor,
    "autocommit": False,
}


def get_connection():
    return pymysql.connect(**DB_CONFIG)


def fetch_all(query: str, params: tuple = ()):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    finally:
        conn.close()


def execute_write(query: str, params: tuple = ()):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            affected = cursor.execute(query, params)
        conn.commit()
        return affected
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PYDANTIC MODELS (INPUT VALIDATION)
# ─────────────────────────────────────────────
class UserRegister(BaseModel):
    username: str = Field(..., min_length=1, max_length=50, description="Unique username")
    email: EmailStr = Field(..., description="Valid email address")
    gender: Literal["Male", "Female", "Other"] = Field(..., description="Gender")
    birthdate: date = Field(..., description="Birthdate in YYYY-MM-DD format")
    country: str = Field(..., min_length=1, max_length=100, description="Country of residence")
    age: int = Field(..., ge=0, le=150, description="Age in years")

    @field_validator("username")
    @classmethod
    def username_no_whitespace(cls, v):
        if v.strip() != v or " " in v:
            raise ValueError("Username cannot contain whitespace")
        return v

    @field_validator("birthdate")
    @classmethod
    def birthdate_not_future(cls, v):
        if v > date.today():
            raise ValueError("Birthdate cannot be in the future")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "john_doe",
                "email": "john@example.com",
                "gender": "Male",
                "birthdate": "1995-06-15",
                "country": "Egypt",
                "age": 30,
            }
        }
    }


class UserUsage(BaseModel):
    username: str = Field(..., min_length=1, max_length=50)
    dataset_identifier: str = Field(..., min_length=1, max_length=100, description="Dataset Identifier")
    project_name: str = Field(..., min_length=1, max_length=200)
    project_category: Literal["analytics", "machine learning", "field research"] = Field(
        ..., description="Must be analytics, machine learning, or field research"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "john_doe",
                "dataset_identifier": "abc-123",
                "project_name": "Air Quality Study",
                "project_category": "analytics",
            }
        }
    }


class MessageResponse(BaseModel):
    success: bool
    message: str


# ─────────────────────────────────────────────
# ROOT — redirect to docs
# ─────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["Utility"])
def health_check():
    """Verify the API and DB connection are alive."""
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            row = cursor.fetchone()
        conn.close()
        return {"status": "healthy", "db_ok": row.get("ok") == 1}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {e}")


# ─────────────────────────────────────────────
# 1. REGISTER A USER
# ─────────────────────────────────────────────
@app.post(
    "/users/register",
    response_model=MessageResponse,
    tags=["1. Users"],
    summary="Register a new user",
)
def register_user(user: UserRegister):
    """Register a new user in the system. All fields are validated."""
    query = """
        INSERT INTO `User` (username, email, gender, birthdate, country, age)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        execute_write(
            query,
            (user.username, user.email, user.gender, user.birthdate, user.country, user.age),
        )
        return MessageResponse(success=True, message=f"User '{user.username}' registered successfully.")
    except pymysql.err.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"User already exists or integrity error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 2. ADD USER USAGE
# ─────────────────────────────────────────────
@app.post(
    "/usage",
    response_model=MessageResponse,
    tags=["2. Usage"],
    summary="Add a new user usage for a given dataset",
)
def add_user_usage(usage: UserUsage):
    """Record that a user is using a specific dataset in a project."""
    query = """
        INSERT INTO Project (Project_name, Project_category, Identifier, Username)
        VALUES (%s, %s, %s, %s)
    """
    try:
        execute_write(
            query,
            (usage.project_name, usage.project_category, usage.dataset_identifier, usage.username),
        )
        return MessageResponse(success=True, message="Usage added successfully.")
    except pymysql.err.IntegrityError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid username or dataset identifier (foreign key failed): {e}",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 3. VIEW USER USAGE
# ─────────────────────────────────────────────
@app.get(
    "/users/{username}/usage",
    tags=["3. Usage"],
    summary="View existing usage information for a user",
)
def view_user_usage(
    username: str = Path(..., min_length=1, max_length=50, description="Username to look up")
):
    """Return every project/dataset combination tied to this user."""
    query = """
        SELECT
            p.Username,
            p.Project_name,
            p.Project_category,
            p.Identifier AS dataset_identifier,
            d.Dataset_name,
            d.Topic,
            d.Publisher
        FROM Project p
        JOIN Data_Set d ON p.Identifier = d.Identifier
        WHERE p.Username = %s
        ORDER BY p.Project_name
    """
    try:
        rows = fetch_all(query, (username,))
        return {"username": username, "count": len(rows), "results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 4. VIEW DATASETS BY ORGANIZATION TYPE
# ─────────────────────────────────────────────
@app.get(
    "/organizations/by-type",
    tags=["4. Organizations"],
    summary="View organizations by type (federal, state, city, etc.)",
)
def view_datasets_by_org_type(
    org_type: str = Query(..., min_length=1, description="e.g. federal, state, city")
):
    """
    Returns organizations matching the given type.

    Note: In the current database, `Data_Set.O_name` is NULL, so datasets cannot be
    linked directly to organization type. This endpoint returns the organizations
    that match, and you can then filter datasets by Publisher separately.
    """
    query = """
        SELECT O_name, O_type
        FROM Organization
        WHERE LOWER(O_type) = LOWER(%s)
        ORDER BY O_name
    """
    try:
        rows = fetch_all(query, (org_type,))
        return {
            "org_type": org_type,
            "count": len(rows),
            "results": rows,
            "note": "Data_Set.O_name is empty in DB; use Publisher for dataset linkage.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 5. TOP 5 CONTRIBUTING ORGANIZATIONS
# ─────────────────────────────────────────────
@app.get(
    "/organizations/top5",
    tags=["5. Organizations"],
    summary="Top 5 contributing organizations (by dataset count)",
)
def top_5_organizations():
    """Returns the 5 organizations (by Publisher) that contributed the most datasets."""
    query = """
        SELECT
            Publisher AS organization,
            COUNT(*) AS dataset_count
        FROM Data_Set
        WHERE Publisher IS NOT NULL AND TRIM(Publisher) <> ''
        GROUP BY Publisher
        ORDER BY dataset_count DESC, organization ASC
        LIMIT 5
    """
    try:
        rows = fetch_all(query)
        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 6. DATASETS BY FORMAT
# ─────────────────────────────────────────────
@app.get(
    "/datasets/by-format",
    tags=["6. Datasets"],
    summary="View datasets available in a given format",
)
def view_datasets_by_format(
    fmt: str = Query(..., min_length=1, description="e.g. CSV, JSON, XML, API"),
    limit: int = Query(100, ge=1, le=500, description="Max rows to return"),
):
    query = """
        SELECT
            d.Identifier,
            d.Dataset_name,
            df.Types,
            df.URL
        FROM Data_Set d
        JOIN DataSet_Formats df ON d.Identifier = df.Dataset_Identifier
        WHERE LOWER(df.Types) = LOWER(%s)
        ORDER BY d.Dataset_name
        LIMIT %s
    """
    try:
        rows = fetch_all(query, (fmt, limit))
        return {"format": fmt, "count": len(rows), "results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 7. DATASETS BY TAG
# ─────────────────────────────────────────────
@app.get(
    "/datasets/by-tag",
    tags=["7. Datasets"],
    summary="View datasets associated with a given tag",
)
def view_datasets_by_tag(
    tag: str = Query(..., min_length=1, description="Tag name"),
    limit: int = Query(100, ge=1, le=500),
):
    query = """
        SELECT
            d.Identifier,
            d.Dataset_name,
            d.Topic,
            t.Tag_Name
        FROM Data_Set d
        JOIN Tag_Associated_DataSet t ON d.Identifier = t.Tag_Identifier
        WHERE LOWER(t.Tag_Name) = LOWER(%s)
        ORDER BY d.Dataset_name
        LIMIT %s
    """
    try:
        rows = fetch_all(query, (tag, limit))
        return {"tag": tag, "count": len(rows), "results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 8. TOTAL DATASETS SUMMARY
# ─────────────────────────────────────────────
@app.get(
    "/datasets/summary",
    tags=["8. Datasets"],
    summary="Total datasets grouped by organization / topic / format / org type",
)
def total_datasets_summary(
    group_by: Literal["organization", "topic", "format", "organization_type"] = Query(
        ..., description="What to group by"
    )
):
    if group_by == "organization":
        query = """
            SELECT Publisher AS organization, COUNT(*) AS total
            FROM Data_Set
            WHERE Publisher IS NOT NULL AND TRIM(Publisher) <> ''
            GROUP BY Publisher
            ORDER BY total DESC, organization ASC
        """
    elif group_by == "topic":
        query = """
            SELECT COALESCE(Topic, 'Unknown') AS topic, COUNT(*) AS total
            FROM Data_Set
            GROUP BY Topic
            ORDER BY total DESC, topic ASC
        """
    elif group_by == "format":
        query = """
            SELECT Types AS format, COUNT(DISTINCT Dataset_Identifier) AS total
            FROM DataSet_Formats
            GROUP BY Types
            ORDER BY total DESC, format ASC
        """
    else:  # organization_type
        query = """
            SELECT O_type AS organization_type, COUNT(*) AS total
            FROM Organization
            WHERE O_type IS NOT NULL AND TRIM(O_type) <> ''
            GROUP BY O_type
            ORDER BY total DESC, organization_type ASC
        """

    try:
        rows = fetch_all(query)
        return {"group_by": group_by, "count": len(rows), "results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 9. TOP 5 DATASETS BY USERS
# ─────────────────────────────────────────────
@app.get(
    "/datasets/top5-by-users",
    tags=["9. Datasets"],
    summary="Top 5 datasets by number of distinct users",
)
def top_5_datasets_by_users():
    query = """
        SELECT
            p.Identifier,
            d.Dataset_name,
            COUNT(DISTINCT p.Username) AS user_count
        FROM Project p
        JOIN Data_Set d ON p.Identifier = d.Identifier
        GROUP BY p.Identifier, d.Dataset_name
        ORDER BY user_count DESC, d.Dataset_name ASC
        LIMIT 5
    """
    try:
        rows = fetch_all(query)
        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 10. USAGE BY PROJECT TYPE
# ─────────────────────────────────────────────
@app.get(
    "/usage/by-project-type",
    tags=["10. Usage"],
    summary="Distribution of dataset usage by project type",
)
def usage_by_project_type():
    query = """
        SELECT Project_category, COUNT(*) AS usage_count
        FROM Project
        GROUP BY Project_category
        ORDER BY usage_count DESC, Project_category ASC
    """
    try:
        rows = fetch_all(query)
        return {"results": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# 11. TOP 10 TAGS PER PROJECT TYPE
# ─────────────────────────────────────────────
@app.get(
    "/tags/top10-by-project-type",
    tags=["11. Tags"],
    summary="Top 10 tags associated with every project type",
)
def top_10_tags_by_project_type():
    category_query = """
        SELECT DISTINCT Project_category
        FROM Project
        WHERE Project_category IS NOT NULL AND TRIM(Project_category) <> ''
        ORDER BY Project_category
    """
    try:
        categories = fetch_all(category_query)
        if not categories:
            return {"results": [], "note": "No project categories found. Add usage records first."}

        result = []
        for row in categories:
            category = row["Project_category"]
            tag_query = """
                SELECT t.Tag_Name, COUNT(*) AS tag_count
                FROM Project p
                JOIN Tag_Associated_DataSet t ON p.Identifier = t.Tag_Identifier
                WHERE p.Project_category = %s
                GROUP BY t.Tag_Name
                ORDER BY tag_count DESC, t.Tag_Name ASC
                LIMIT 10
            """
            tags = fetch_all(tag_query, (category,))
            result.append({"project_category": category, "top_tags": tags})

        return {"results": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# LOCAL ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)
