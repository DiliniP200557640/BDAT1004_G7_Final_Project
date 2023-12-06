from flask import Flask, render_template, g
import psycopg2

app = Flask(__name__)

# Database configuration
db_config = {
    "host": "default-workgroup.721630359816.us-east-1.redshift-serverless.amazonaws.com",
    "port": 5439,
    "database": "dev",
    "user": "admin",
    "password": "*********",
}


def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(**db_config)
    return g.db

   
@app.route('/')
def index():
    return render_template('index.html')

    
@app.route('/datadisplay')
def datadisplay():
    cursor = get_db().cursor()
    cursor.execute("SELECT TOP 20 * FROM sales.sales_data")
    data = cursor.fetchall()
    return render_template('datadisplay.html', data=data)

@app.route('/sales')
def multiple_charts():
    cursor = get_db().cursor()
    cursor.execute("SELECT * FROM sales.sales_data")
    result = cursor.fetchall()
    data_list = [list(row) for row in result]
    return render_template('sales.html' , data=data_list)


    
if __name__ == "__main__":
    app.run(debug=False)