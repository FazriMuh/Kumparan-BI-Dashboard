from flask import Flask, render_template, request, redirect, flash, jsonify
import csv
import threading
import matplotlib.pyplot as plt
from data_process import story_published, generate_bar_line_plot, generate_bar_line_plot2, generate_bar_line_plot3,generate_bar_line_plot4
import pandas as pd
import os
import mysql.connector
from sqlalchemy import create_engine

app = Flask(__name__)
app.secret_key = 'secret_key'



engine = create_engine('mysql+mysqlconnector://goja:root@localhost/kumparan')


plt.switch_backend('Agg')

# Decorator for matplotlib
def generate_plot_async(func):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
    return wrapper



@app.route('/')
def index():
    sum_story_published = story_published()
    sum_story_published = len(sum_story_published)
    
    query = """SELECT COUNT(DISTINCT user_alias_id) AS total_unique_users
        FROM daily_page_view_activity"""
    df = pd.read_sql(query, con=engine)
    total_unique_users = df.iloc[0]['total_unique_users']
    
    query = """SELECT COUNT(DISTINCT session_id) AS total_session_id
        FROM daily_page_view_activity"""
    df = pd.read_sql(query, con=engine)
    total_session_id = df.iloc[0]['total_session_id']
    
    unique_user = generate_bar_line_plot()
    unique_visit = generate_bar_line_plot2()
    page_view = generate_bar_line_plot3()
    unique_story = generate_bar_line_plot4()
    
    return render_template('dashboard.html', sum_story_published = sum_story_published,total_unique_users=total_unique_users, 
                           total_session_id = total_session_id, unique_user = unique_user, unique_visit=unique_visit, 
                           page_view=page_view, unique_story = unique_story)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
