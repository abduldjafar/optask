BRONZE_TABLES = {
    "students": {
        "file_type": "parquet",
        "primary_key": ["student_id", "updated_at"],
        "columns": ["student_id", "student_name", "class_id", "grade_level", "enrollment_status", "updated_at"]
    },
    "attendance": {
        "file_type": "parquet",
        "primary_key": ["attendance_id", "created_at"],
        "columns": ["attendance_id", "student_id", "attendance_date", "status", "created_at"]
    },
    "assessments": {
        "file_type": "parquet",
        "primary_key": ["assessment_id", "created_at"],
        "columns": ["assessment_id", "student_id", "subject", "score", "max_score", "assessment_date", "created_at"]
    }
}
