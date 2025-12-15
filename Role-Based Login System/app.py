from flask import Flask, render_template, request, redirect, url_for, session
from flask_mysqldb import MySQL
import MySQLdb.cursors

app = Flask(__name__)
app.secret_key = "secret123"

#hindi na db connector 😊
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'rolebased_db'

mysql = MySQL(app)

#login route
@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
def login():
    msg = ""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(
            "SELECT * FROM account WHERE username=%s AND password=%s",
            (username, password)
        )
        account = cursor.fetchone()

        if account:
            session['loggedin'] = True
            session['id'] = account['acctno']
            session['username'] = account['username']
            session['role'] = account['role']
            return redirect(url_for('home'))
        else:
            msg = "Invalid username or password!"

    return render_template('login.html', msg=msg)

#reg route
@app.route('/register', methods=['GET', 'POST'])
def register():
    msg = ""
    if request.method == 'POST':
        name = request.form['name']
        sex = request.form['sex']
        dob = request.form['dob']
        username = request.form['username']
        password = request.form['password']

        cursor = mysql.connection.cursor()
        cursor.execute(
            "INSERT INTO account (name, sex, dob, username, password, role) VALUES (%s,%s,%s,%s,%s,'user')",
            (name, sex, dob, username, password)
        )
        mysql.connection.commit()
        msg = "Account created successfully!"

    return render_template('register.html', msg=msg)

#home route
@app.route('/home')
def home():
    if 'loggedin' not in session:
        return redirect(url_for('login'))

    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    if session['role'] == 'admin':
        cursor.execute("SELECT * FROM account WHERE role='user'")
        users = cursor.fetchall()
        return render_template('home.html', users=users, admin=True)

    cursor.execute("SELECT * FROM account WHERE acctno=%s", (session['id'],))
    user = cursor.fetchone()
    return render_template('home.html', user=user, admin=False)

#log out route
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

if __name__ == "__main__":
    app.run(debug=True)
