from flask import Flask, jsonify, render_template
from c21server.dashboard.job_stats import get_jobs_stats

app = Flask(__name__)


@app.route('/dashboard', methods=['GET'])
def index():
    job_information = get_jobs_stats()
    return render_template(
        'index.html', jobs_waiting=job_information['num_jobs_waiting'],
        jobs_in_progress=job_information['num_jobs_in_progress'],
        jobs_done=job_information['num_jobs_done'],
        jobs_total=job_information['jobs_total'],
        clients_total=job_information['clients_total']
    )


@app.route('/data', methods=['GET'])
def get_dashboard_data():
    job_information = get_jobs_stats()
    return jsonify(job_information), 200
