import streamlit as st
import pymongo
import pandas as pd
import datetime  # Import datetime module
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest

# 连接到 MongoDB
client = pymongo.MongoClient("")

# 设置数据库和集合
task_db = client["taskTracker"]
task_collection = task_db["usersubmissions"]

affiliate_db = client["affiliaterewards"]
referral_collection = affiliate_db["referralListForEachUser"]

swap_airdrops_collection = affiliate_db["swapAirdrops"]

# 新的 MongoDB 连接
new_client = pymongo.MongoClient("")
test_db = new_client["test"]
userfaucets_collection = test_db["userfaucets"]

# Google Analytics API 认证
def initialize_ga_api():
    credentials = service_account.Credentials.from_service_account_file(
        'ga-data-391318-56f55ffd99ee.json',  # 替换为您的 credentials.json 文件路径
        scopes=['https://www.googleapis.com/auth/analytics.readonly']
    )
    client = BetaAnalyticsDataClient(credentials=credentials)
    return client

# 从 Google Analytics 获取数据（GA4）
def get_ga_data(client):
    request = RunReportRequest(
        property=f"properties/317912804",  # 替换为您的 GA4 Property ID
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="activeUsers"), Metric(name="sessions")],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="yesterday")]
    )
    response = client.run_report(request)

    # 解析响应并格式化为 DataFrame
    rows = []
    for row in response.rows:
        rows.append({
            "Date": row.dimension_values[0].value,
            "Active Users": row.metric_values[0].value,
            "Sessions": row.metric_values[1].value
        })

    return pd.DataFrame(rows)


def load_data():
    # 加载其他数据
    task_data = load_task_data()
    referral_data = load_referral_data()
    userfaucets_data, validation_counts = load_userfaucets_data()
    swap_airdrops_data = load_swap_airdrops_data()
    
    # 获取有 submission 的 pubKey 并比对
    submission_pubkeys = load_submission_user_data()
    missing_pubkeys_df = compare_pubkeys_with_userfaucets(submission_pubkeys)
    
    # 加载 GA 数据
    client = initialize_ga_api()
    ga_data = get_ga_data(client)
    
    return task_data, referral_data, userfaucets_data, validation_counts, swap_airdrops_data, ga_data, missing_pubkeys_df





# 加载 taskTracker 数据
def load_task_data():
    data = list(task_collection.find())
    formatted_data = []
    for record in data:
        pub_key = record.get("pubKey", "N/A")
        created_at = record.get("createdAt", "N/A")
        updated_at = record.get("updatedAt", "N/A")
        submissions = record.get("submissions", {})
        
        for date, submission in submissions.items():
            score = submission.get("score", 0)  # Use 0 as default value for summation
            formatted_data.append({
                "pubKey": pub_key,
                "Date": date,
                "Score": score,
                "Created At": created_at,
                "Updated At": updated_at
            })
    
    df = pd.DataFrame(formatted_data)
    if not df.empty:
        # Sum scores by date
        df = df.groupby("Date", as_index=False).agg({"Score": "sum"})
    
    return df

def load_submission_user_data():
    # 从 task_collection 中提取有 submission 的 pubKey 和日期
    task_data = list(task_collection.find())
    submission_pubkeys = {}
    
    for record in task_data:
        pub_key = record.get("pubKey", "N/A")
        submissions = record.get("submissions", {})
        
        for date, submission in submissions.items():
            if pub_key != "N/A":
                if date not in submission_pubkeys:
                    submission_pubkeys[date] = set()
                submission_pubkeys[date].add(pub_key)
    
    return submission_pubkeys

def compare_pubkeys_with_userfaucets(submission_pubkeys):
    # 从 userfaucets_collection 中提取所有的 walletAddress
    userfaucets_data = list(userfaucets_collection.find({}, {"walletAddress": 1}))
    userfaucets_pubkeys = set(record.get("walletAddress", "N/A") for record in userfaucets_data)

    missing_pubkeys_by_date = []
    
    for date, pubkeys in submission_pubkeys.items():
        missing_pubkeys = pubkeys - userfaucets_pubkeys
        total_pubkeys = len(pubkeys)
        total_missing = len(missing_pubkeys)
        missing_ratio = total_missing / total_pubkeys if total_pubkeys > 0 else 0
        
        missing_pubkeys_by_date.append({
            "Date": date,
            "Missing PubKeys": list(missing_pubkeys),
            "Total Missing": total_missing,
            "Total PubKeys": total_pubkeys,
            "Missing Ratio": round(missing_ratio * 100, 2)  # 转换为百分比并保留两位小数
        })
    
    return pd.DataFrame(missing_pubkeys_by_date)



# 加载 affiliaterewards 数据
def load_referral_data():
    data = list(referral_collection.find())
    formatted_data = []
    for record in data:
        wallet_address = record.get("walletAddress", "N/A")
        email = record.get("email", "N/A")
        referral_code = record.get("referralCode", "N/A")
        total_referrals = record.get("totalReferrals", "N/A")
        created_at = record.get("createdAt", "N/A")
        updated_at = record.get("updatedAt", "N/A")
        referrals = record.get("referrals", {})
        
        for date, referred_users in referrals.items():
            for user in referred_users:
                if user:
                    formatted_data.append({
                        "Wallet Address": wallet_address,
                        "Email": email,
                        "Referral Code": referral_code,
                        "Referral Date": date,
                        "Referred User": user,
                        "Total Referrals": total_referrals,
                        "Created At": created_at,
                        "Updated At": updated_at
                    })
    
    return pd.DataFrame(formatted_data)

def load_swap_airdrops_data():
    data = list(swap_airdrops_collection.find())
    formatted_data = []
    for record in data:
        created_at = record.get("createdAt", "N/A")
        is_keep_my_airdrop = record.get("isKeepMyAirdrop", False)
        
        # Convert created_at to date only
        if isinstance(created_at, datetime.datetime):
            created_at = created_at.strftime('%Y-%m-%d')

        formatted_data.append({
            "Created At": created_at,
            "Is Keep My Airdrop": is_keep_my_airdrop
        })
    
    df = pd.DataFrame(formatted_data)
    
    # Group by date and count the number of true/false for isKeepMyAirdrop
    airdrop_counts = df.groupby(["Created At", "Is Keep My Airdrop"]).size().unstack(fill_value=0)
    
    # Rename the columns to match the desired labels
    airdrop_counts.columns = ['# of ppl swapped to KOII', '# of ppl keep their altcoin']
    
    return airdrop_counts

# 加载 userfaucets 数据
def load_userfaucets_data():
    data = list(userfaucets_collection.find())
    formatted_data = []
    for record in data:
        wallet_address = record.get("walletAddress", "N/A")
        discord_id = record.get("discordId", "N/A")
        discord_validation = record.get("discordValidation", "N/A")
        email_validation = record.get("emailValidation", "N/A")
        phone_validation = record.get("phoneValidation", "N/A")
        twitter_validation = record.get("twitterValidation", "N/A")
        created_at = record.get("createdAt", "N/A")
        updated_at = record.get("updatedAt", "N/A")
        email_address = record.get("emailAddress", "N/A")
        twitter_id = record.get("twitterId", "N/A")
        github_id = record.get("githubId", "N/A")
        github_validation = record.get("githubValidation", "N/A")
        referral = record.get("referral", "N/A")

        # Convert created_at and updated_at to date only
        if isinstance(created_at, datetime.datetime):
            created_at = created_at.strftime('%Y-%m-%d')
        if isinstance(updated_at, datetime.datetime):
            updated_at = updated_at.strftime('%Y-%m-%d')

        formatted_data.append({
            "Wallet Address": wallet_address,
            "Discord ID": discord_id,
            "Discord Validation": discord_validation,
            "Email Validation": email_validation,
            "Phone Validation": phone_validation,
            "Twitter Validation": twitter_validation,
            "Created At": created_at,
            "Updated At": updated_at,
            "Email Address": email_address,
            "Twitter ID": twitter_id,
            "GitHub ID": github_id,
            "GitHub Validation": github_validation,
            "Referral": referral
        })
    
    df = pd.DataFrame(formatted_data)
    
    # Group by date to get the count of new users per day
    new_users_count = df.groupby("Created At").size().reset_index(name="New Users")
    
    # Calculate counts for validations
    email_claimed = df[df['Email Validation'] == 'CLAIMED'].groupby('Created At').size().reset_index(name='Email Claimed')
    twitter_claimed = df[df['Twitter Validation'] == 'CLAIMED'].groupby('Created At').size().reset_index(name='Twitter Claimed')
    discord_claimed = df[df['Discord Validation'] == 'CLAIMED'].groupby('Created At').size().reset_index(name='Discord Claimed')
    
    # Merge the counts into a single DataFrame
    validation_counts = new_users_count.merge(email_claimed, on='Created At', how='left') \
                                       .merge(twitter_claimed, on='Created At', how='left') \
                                       .merge(discord_claimed, on='Created At', how='left')
    
    # Fill NaN values with 0
    validation_counts = validation_counts.fillna(0)
    
    return df, validation_counts

# 加载数据
task_data, referral_data, userfaucets_data, validation_counts, swap_airdrops_data, ga_data, missing_pubkeys_df = load_data()

# 显示缺失的 pubKey 数据
st.subheader("Missing PubKeys in UserFaucets")
st.write(missing_pubkeys_df)


# 显示 taskTracker 数据
st.write("Task Submissions Data")
st.write(task_data)

# 显示 referralListForEachUser 数据
st.write("Referral Data")
st.write(referral_data)

# 显示 userfaucets 数据
st.write("User Faucets Data")
st.write(userfaucets_data)

# 显示 Google Analytics 数据
st.write("Google Analytics Data")
st.write(ga_data)

# 显示 swapAirdrops 数据
st.write("Swap Airdrops Data")
st.write(swap_airdrops_data)

# 数据可视化
if not task_data.empty:
    st.subheader("Task Submissions Over Time")
    st.line_chart(task_data.set_index('Date'))

if not referral_data.empty:
    st.subheader("Total Referrals by Date")
    st.bar_chart(referral_data.groupby('Referral Date').size())

# Filter the data to include only the past 30 days, excluding today
thirty_days_ago = datetime.datetime.now() - datetime.timedelta(days=30)
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

# Convert dates to strings for comparison
thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%d')
yesterday_str = yesterday.strftime('%Y-%m-%d')

validation_counts_filtered = validation_counts[
    (validation_counts['Created At'] >= thirty_days_ago_str) & 
    (validation_counts['Created At'] <= yesterday_str)
]

# Data visualization for past 30 days, excluding today
if not validation_counts_filtered.empty:
    st.subheader("User Faucets by Created Date (Last 30 Days, Excluding Today)")
    st.bar_chart(validation_counts_filtered.set_index('Created At'))


    # Display the validation counts table
    st.subheader("Validation Counts Per Day")
    st.write(validation_counts)

if not ga_data.empty:
    st.subheader("Google Analytics: Sessions and Active Users Over Time")
    st.line_chart(ga_data.set_index('Date'))

# 实时数据监控
st.subheader("Real-Time Monitoring")
if st.button("Refresh Data"):
    task_data, referral_data, userfaucets_data, validation_counts, swap_airdrops_data, ga_data = load_data()
    
    st.write("Task Submissions Data")
    st.write(task_data)
    
    if not task_data.empty:
        st.line_chart(task_data.set_index('Date'))
    
    st.write("Referral Data")
    st.write(referral_data)
    
    if not referral_data.empty:
        st.bar_chart(referral_data.groupby('Referral Date').size())
    
    st.write("User Faucets Data")
    st.write(userfaucets_data)
    
    if not userfaucets_data.empty:
        st.bar_chart(validation_counts.set_index('Created At'))
        st.subheader("Validation Counts Per Day")
        st.write(validation_counts)
    
    st.write("Swap Airdrops Data")
    st.write(swap_airdrops_data)
    
    st.write("Google Analytics Data")
    st.write(ga_data)
    
    if not ga_data.empty:
        st.line_chart(ga_data.set_index('Date'))


    st.subheader("Missing PubKeys in UserFaucets")
    st.write(missing_pubkeys_df)