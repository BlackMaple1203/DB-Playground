import sqlite3
import os
import re

DB_FILE = 'review.db'
SQL_DIR = 'school'
SQL_FILES = ['STUDENTS.sql', 'TEACHERS.sql', 'COURSES.sql', 'CHOICES.sql']

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON;")
        
        for sql_file in SQL_FILES:
            file_path = os.path.join(SQL_DIR, sql_file)
            if os.path.exists(file_path):
                print(f"Processing {sql_file}...")
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    # Read file content
                    content = f.read()
                    
                    # Remove 'use school;' which is not supported in SQLite
                    content = re.sub(r'use\s+school\s*;', '', content, flags=re.IGNORECASE)
                    
                    # Add IF EXISTS to DROP TABLE
                    content = re.sub(r'drop\s+table\s+(\w+);', r'DROP TABLE IF EXISTS \1;', content, flags=re.IGNORECASE)
                    
                    # Execute the script
                    cursor.executescript(content)
                    conn.commit()
                    print(f"Successfully executed {sql_file}")
            else:
                print(f"Warning: File not found: {file_path}")
                
        print("数据库已成功初始化！")
    except Exception as e:
        print(f"初始化失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
