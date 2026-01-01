import streamlit as st
import sqlite3
import pandas as pd
import json
import os
import re
import concurrent.futures

try:
    from streamlit_ace import st_ace
    ACE_AVAILABLE = True
except ImportError:
    ACE_AVAILABLE = False

# 配置页面
st.set_page_config(page_title="SQL 期末复习系统", layout="wide")

# --- Custom CSS for Sidebar ---
st.markdown("""
    <style>
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label {
        padding: 10px 10px; /* 增加内边距 */
        margin-bottom: 8px; /* 增加题目之间的间距 */
        border-radius: 8px;
        background-color: rgba(255, 255, 255, 0.05); /* 轻微背景色 */
        border: 1px solid rgba(255, 255, 255, 0.1);
        transition: all 0.2s ease;
    }
    
    /* 鼠标悬停效果 */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:hover {
        background-color: rgba(255, 255, 255, 0.15);
        border-color: rgba(255, 255, 255, 0.3);
        transform: translateX(5px); /* 悬停时轻微右移 */
    }

    /* 调整单选按钮文字样式 */
    section[data-testid="stSidebar"] .stRadio label p {
        font-size: 15px;
        line-height: 1.4;
    }
    
    /* 隐藏默认的单选圆圈，让整个块看起来像按钮 (可选，视个人喜好) */
    /* section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label div:first-child {
        display: none;
    } */
    </style>
""", unsafe_allow_html=True)

DB_FILE = 'review.db'
ANSWERS_FILE = 'answers.json'
SQL_DIR = 'school'
SQL_FILES = ['STUDENTS.sql', 'TEACHERS.sql', 'COURSES.sql', 'CHOICES.sql']

def get_connection():
    return sqlite3.connect(DB_FILE)

def init_db():
    """初始化数据库"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON;")
        
        # Create history table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS USER_HISTORY (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER,
                user_sql TEXT,
                is_correct BOOLEAN,
                error_message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        for sql_file in SQL_FILES:
            file_path = os.path.join(SQL_DIR, sql_file)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    # Remove 'use school;'
                    content = re.sub(r'use\s+school\s*;', '', content, flags=re.IGNORECASE)
                    # Add IF EXISTS to DROP TABLE
                    content = re.sub(r'drop\s+table\s+(\w+);', r'DROP TABLE IF EXISTS \1;', content, flags=re.IGNORECASE)
                    cursor.executescript(content)
                    conn.commit()
        
        # 清空缓存
        st.cache_data.clear()
        st.success("数据库已成功初始化/重置！")
    except Exception as e:
        st.error(f"初始化失败: {e}")
    finally:
        conn.close()

def init_history_table():
    """Ensure history table exists without resetting everything"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS USER_HISTORY (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER,
                user_sql TEXT,
                is_correct BOOLEAN,
                error_message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    except Exception as e:
        st.error(f"History table init failed: {e}")
    finally:
        conn.close()

def save_history(question_id, user_sql, is_correct, error_message=None):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO USER_HISTORY (question_id, user_sql, is_correct, error_message)
            VALUES (?, ?, ?, ?)
        """, (question_id, user_sql, is_correct, error_message))
        conn.commit()
    except Exception as e:
        st.error(f"Failed to save history: {e}")
    finally:
        conn.close()

def get_history(question_id):
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT user_sql, is_correct, error_message, timestamp FROM USER_HISTORY WHERE question_id = ? ORDER BY timestamp DESC",
            conn,
            params=(question_id,)
        )
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

def get_solved_questions():
    conn = get_connection()
    try:
        # Get distinct question_ids where is_correct is true
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT question_id FROM USER_HISTORY WHERE is_correct = 1")
        solved_ids = {row[0] for row in cursor.fetchall()}
        return solved_ids
    except Exception:
        return set()
    finally:
        conn.close()

def load_questions():
    questions = []
    
    # Load questions and answers from JSON
    if os.path.exists(ANSWERS_FILE):
        with open(ANSWERS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for idx, item in enumerate(data):
                questions.append({
                    "id": idx,
                    "title": item.get("question", ""),
                    "description": item.get("question", ""),
                    "answer_sql": item.get("sql", "")
                })
            
    return questions

def run_query(query):
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

def run_query_with_timeout(query, timeout_seconds=2):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(run_query, query)
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            return None, "Timeout"
        except Exception as e:
            return None, str(e)

@st.cache_data(ttl=300)  # 缓存5分钟
def get_table_list():
    """获取数据库表列表（带缓存）"""
    conn = get_connection()
    try:
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('sqlite_sequence', 'USER_HISTORY');",
            conn
        )
        return tables['name'].tolist()
    except Exception as e:
        st.error(f"获取表列表失败: {e}")
        return []
    finally:
        conn.close()

@st.cache_data(ttl=3000)
def get_table_data(table_name):
    """获取指定表的数据（带缓存）"""
    conn = get_connection()
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df
    except Exception as e:
        st.error(f"读取表 {table_name} 失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# --- 侧边栏 ---
# Ensure history table exists on startup
init_history_table()

with st.sidebar.expander("功能菜单", expanded=False):
    if st.button("重置/初始化数据库"):
        init_db()

questions = load_questions()
if not questions:
    st.error(f"没有找到题目配置文件 {ANSWERS_FILE}")
    st.stop()

# Initialize session state
if 'current_index' not in st.session_state:
    st.session_state.current_index = 0

# Ensure index is valid
if st.session_state.current_index >= len(questions):
    st.session_state.current_index = 0

# 题目选择
solved_ids = get_solved_questions()
question_titles = []
for q in questions:
    prefix = "✅ " if q['id'] in solved_ids else ""
    question_titles.append(f"{prefix}{q['id'] + 1}. {q['title']}")

def prev_question():
    if st.session_state.current_index > 0:
        st.session_state.current_index -= 1

def next_question():
    if st.session_state.current_index < len(questions) - 1:
        st.session_state.current_index += 1

def update_index():
    # Find index based on selected title
    st.session_state.current_index = question_titles.index(st.session_state.question_list)

# List selection
selected_title = st.sidebar.radio(
    "题目列表",
    question_titles,
    index=st.session_state.current_index,
    key="question_list",
    on_change=update_index
)

current_q = questions[st.session_state.current_index]

# Navigation buttons
col_nav1, col_nav2, col_nav3 = st.columns([1, 4, 1])
with col_nav1:
    st.button("上一题", on_click=prev_question, disabled=st.session_state.current_index == 0, width='stretch')
with col_nav3:
    st.button("下一题", on_click=next_question, disabled=st.session_state.current_index == len(questions) - 1, width='stretch')

st.markdown(f"### 题目 {current_q['id'] + 1}: {current_q['title']}")
# st.info(current_q['description']) 

# Layout control
split_ratio = st.sidebar.slider("调整布局比例 (左:右)", 0.1, 0.9, 0.5, 0.05)

# --- Row 1: User Answer & DB Schema ---
col1, col2 = st.columns([split_ratio, 1-split_ratio])

with col1:
    if ACE_AVAILABLE:
        initial_sql = st.session_state.get(f"sql_{current_q['id']}", "")
        user_sql = st_ace(
            value=initial_sql,
            language="sql",
            theme="monokai",
            key=f"sql_editor_{current_q['id']}",
            min_lines=20,
            wrap=True,
            auto_update=True,
            show_gutter=True,
            show_print_margin=False,
            # enable_live_autocomplete=True, # Removed due to TypeError
            # enable_basic_autocompletion=True,
            # enable_snippets=True,
        )
        st.session_state[f"sql_{current_q['id']}"] = user_sql
    else:
        st.warning("缺少 streamlit-ace 依赖，暂时使用普通输入框。安装: pip install streamlit-ace")
        user_sql = st.text_area("输入 SQL 语句", height=150, key=f"sql_{current_q['id']}")
    
    col_run, col_submit = st.columns([1, 1])
    with col_run:
        run_clicked = st.button("运行", key=f"run_{current_q['id']}", width='stretch')
    with col_submit:
        submit_clicked = st.button("提交", key=f"submit_{current_q['id']}", type="primary", width='stretch')

    if run_clicked or submit_clicked:
        if user_sql.strip():
            user_df, error = run_query(user_sql)
            is_correct = False
            error_msg = error
            
            if error:
                st.error(f"执行出错:\n{error}")
            else:
                # Limit to 10 rows for display
                display_user_df = user_df.head(10)
                
                # 处理重复列名，防止 st.dataframe 报错
                if len(display_user_df.columns) != len(set(display_user_df.columns)):
                    new_cols = []
                    seen = set()
                    for col in display_user_df.columns:
                        c = col
                        i = 1
                        while c in seen:
                            c = f"{col}.{i}"
                            i += 1
                        seen.add(c)
                        new_cols.append(c)
                    display_user_df.columns = new_cols

                st.dataframe(display_user_df, width='stretch')
                
                if len(user_df) > 10:
                    st.caption(f"显示前 10 行 (共 {len(user_df)} 行)")
                else:
                    st.caption(f"共 {len(user_df)} 行")
                
                # 只有点击提交按钮时才进行比对和保存记录
                if submit_clicked:
                    # 简单的结果比对 (Compare full dataframes, not just displayed ones)
                    if current_q['answer_sql']:
                        expected_df, expected_error = run_query_with_timeout(current_q['answer_sql'])
                        
                        if expected_error == "Timeout":
                            st.warning("参考答案加载超时，本次提交不进行判题，仅保存记录。")
                            error_msg = "Reference answer timeout, validation skipped."
                        elif expected_df is not None:
                            try:
                                # 忽略列名进行比对：统一重命名列名为索引
                                expected_df.columns = range(len(expected_df.columns))
                                user_df.columns = range(len(user_df.columns))

                                # Sort and compare
                                pd.testing.assert_frame_equal(
                                    expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True),
                                    user_df.sort_values(by=list(user_df.columns)).reset_index(drop=True),
                                    check_dtype=False
                                )
                                st.toast("✅ 结果正确！")
                                is_correct = True
                            except AssertionError:
                                st.toast("❌ 结果与预期不完全一致，请检查数据或排序。")
                                error_msg = "Result mismatch"
                            except Exception as e:
                                st.toast(f"⚠️ 无法自动比对结果: {e}")
                                error_msg = str(e)
                
                    # Save history
                    save_history(current_q['id'], user_sql, is_correct, error_msg)
            
        else:
            st.warning("请输入 SQL 语句")

with col2:
    st.subheader("当前数据库表结构")
    
    # 使用缓存的表列表
    table_list = get_table_list()
    if table_list:
        st.write("现有表:", table_list)
        
        selected_table = st.selectbox("预览表数据", table_list)
        if selected_table:
            # 使用缓存的表数据
            df = get_table_data(selected_table)
            if not df.empty:
                st.dataframe(df.head(10), width='stretch')
                if len(df) > 10:
                    st.caption(f"显示前 10 行 (共 {len(df)} 行)")
                else:
                    st.caption(f"共 {len(df)} 行")
    else:
        st.error("无法读取数据库结构")

st.divider()

# --- Row 2: History & Expected Result ---
col3, col4 = st.columns([split_ratio, 1-split_ratio])

with col3:
    st.subheader("提交记录")
    history_df = get_history(current_q['id'])
    if not history_df.empty:
        for _, row in history_df.iterrows():
            status_icon = "✅" if row['is_correct'] else "❌"
            with st.expander(f"{status_icon} {row['timestamp']}"):
                st.code(row['user_sql'], language='sql')
                if not row['is_correct'] and row['error_message']:
                    st.error(f"Error: {row['error_message']}")
    else:
        st.caption("暂无历史记录")

with col4:
    st.subheader("预期结果 (参考答案)")
    if current_q['answer_sql']:
        if st.checkbox("显示预期结果", value=True):
            expected_df, error = run_query_with_timeout(current_q['answer_sql'])
            if error == "Timeout":
                st.warning("答案暂时无法加载")
            elif error:
                st.error(f"参考答案执行出错: {error}")
            else:
                # Limit to 10 rows
                display_df = expected_df.head(10)
                st.dataframe(display_df, width='stretch')
                if len(expected_df) > 10:
                    st.caption(f"显示前 10 行 (共 {len(expected_df)} 行)")
                else:
                    st.caption(f"共 {len(expected_df)} 行")
    else:
        st.warning("暂无参考答案")
