import matplotlib.pyplot as plt
import pandas as pd
import base64
from io import BytesIO
import mysql.connector
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://goja:root@localhost/kumparan')

#Q1
def story_published():
    query = """SELECT *
                FROM stories_info
                WHERE YEAR(story_published) = 2020 AND story_deleted IS NULL"""
    df = pd.read_sql(query, con=engine)
    return df

#Q2
def jan_feb_collection():
    query = """SELECT IFNULL(collection_id, 'NULL') AS collection_id, COUNT(*) AS count
            FROM stories_info
            WHERE YEAR(story_published) = 2020 AND MONTH(story_published) IN (1, 2)
            GROUP BY collection_id"""
    df = pd.read_sql(query, con=engine)
    return df

#Q3
def author_w_publisher():
    query = """SELECT YEAR(story_published) AS year,
                    MONTH(story_published) AS month,
                    author_id,
                    COUNT(*) AS count
                FROM stories_info
                INNER JOIN writer_classification
                ON stories_info.publisher_id = writer_classification.object_id
                WHERE writer_classification.object_type = 1
                AND YEAR(story_published) = 2020
                GROUP BY YEAR(story_published), MONTH(story_published), author_id"""
    df = pd.read_sql(query, con=engine)
    return df


#Q4

query_daily_act = "SELECT * FROM daily_page_view_activity"
daily_act = pd.read_sql(query_daily_act, con=engine)
daily_act['month'] = daily_act['jkt_time'].dt.to_period('M')

def count_unique_items(group, item_column):
    unique_items = set()
    current_month = group.name
    last_seen_month = {}
    
    for item in group[item_column]:
        if item not in last_seen_month or last_seen_month[item] != current_month - 1:
            unique_items.add(item)
        last_seen_month[item] = current_month
    
    return len(unique_items)

#unique user
def unique_users_per_month():
    unique_users_per_month = (
        daily_act
        .groupby('month')
        .apply(lambda x: count_unique_items(x, 'user_alias_id'))
        .reset_index(name='unique_users')
    )
    return unique_users_per_month

def generate_bar_line_plot():
    df = unique_users_per_month()
    df['month'] = pd.PeriodIndex(df['month'], freq='M').to_timestamp()
    df['month'] = df['month'].dt.strftime('%B')

    plt.figure(figsize=(10, 6))
    bars = plt.bar(df['month'], df['unique_users'], color='#00B1B2')

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2 - 0.1, yval + 100, yval, va='center')

    plt.plot(df['month'], df['unique_users'], color='#EA1F48', marker='o', linestyle='-')
    plt.title('Unique Users per Month')
    plt.xlabel('Month')
    plt.ylabel('Unique Users')
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    bar_line_data = base64.b64encode(buffer.read()).decode('utf-8')

    return bar_line_data



#unique visit
def unique_visit_per_month():
    unique_visit_per_month = (
        daily_act
        .groupby('month')
        .apply(lambda x: count_unique_items(x, 'session_id'))
        .reset_index(name='unique_visit')
    )
    return unique_visit_per_month


def generate_bar_line_plot2():
    df = unique_visit_per_month()
    df['month'] = pd.PeriodIndex(df['month'], freq='M').to_timestamp()
    df['month'] = df['month'].dt.strftime('%B')

    plt.figure(figsize=(10, 6))
    bars = plt.bar(df['month'], df['unique_visit'], color='#00B1B2')

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2 - 0.1, yval + 50, yval, va='center')
        
    plt.plot(df['month'], df['unique_visit'], color='#EA1F48', marker='o', linestyle='-')
    plt.title('Unique Visits per Month')
    plt.xlabel('Month')
    plt.ylabel('Unique Visits')

    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    bar_line_data = base64.b64encode(buffer.read()).decode('utf-8')

    return bar_line_data


#page view
def count_unique_pageviews(group):
    unique_combinations = set()
    current_month = group.name
    last_seen_month = {}
    
    for _, row in group.iterrows():
        combination = (row['session_id'], row['user_alias_id'], row['story_id'])
        if combination not in last_seen_month or last_seen_month[combination] != current_month - 1:
            unique_combinations.add(combination)
        last_seen_month[combination] = current_month
    
    return len(unique_combinations)

def total_pageviews_per_month():
    total_pageviews_per_month = (
        daily_act
        .groupby('month')
        .apply(count_unique_pageviews)
        .reset_index(name='unique_pageviews')
    )
    return total_pageviews_per_month

def generate_bar_line_plot3():
    df = total_pageviews_per_month()
    df['month'] = pd.PeriodIndex(df['month'], freq='M').to_timestamp()
    df['month'] = df['month'].dt.strftime('%B')
    
    plt.figure(figsize=(10, 6))
    
    bars = plt.bar(df['month'], df['unique_pageviews'], color='#00B1B2')
    
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2 - 0.1, yval + 50, yval, va='center')
    
    plt.plot(df['month'], df['unique_pageviews'], color='#EA1F48', marker='o', linestyle='-')
    
    plt.title('Unique Page Views per Month')
    plt.xlabel('Month')
    plt.ylabel('Unique Page Views')
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plot_data = base64.b64encode(buffer.read()).decode('utf-8')
    
    return plot_data

# unique story
last_seen_month = {}
def count_unique_stories(group):
    unique_stories = set()
    current_month = group.name
    for story in group['story_id']:
        if story not in last_seen_month or last_seen_month[story] != current_month - 1:
            unique_stories.add(story)
        last_seen_month[story] = current_month
    return len(unique_stories)

def unique_stories_per_month():
    unique_stories_per_month = (
        daily_act
        .groupby('month')
        .apply(count_unique_stories)
        .reset_index(name='unique_stories')
    )
    return unique_stories_per_month

def generate_bar_line_plot4():
    df = unique_stories_per_month()
    
    df['month'] = pd.PeriodIndex(df['month'], freq='M').to_timestamp()
    df['month'] = df['month'].dt.strftime('%B')
    
    plt.figure(figsize=(10, 6))
    
    bars = plt.bar(df['month'], df['unique_stories'], color='#00B1B2')
    
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, f'{yval:,}', 
                 va='bottom', ha='center', fontsize=10)
    
    line = plt.plot(df['month'], df['unique_stories'], color='#EA1F48', marker='o', linestyle='-')
    
    plt.title('Monthly Unique Story', fontsize=14)
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Unique Stories', fontsize=12)
    plt.yticks(fontsize=10)
    
    # Adjust y-axis to leave space for labels
    plt.ylim(0, max(df['unique_stories']) * 1.1)
    
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=300)
    buffer.seek(0)
    plot_data = base64.b64encode(buffer.read()).decode('utf-8')
    
    return plot_data