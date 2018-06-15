from bs4 import BeautifulSoup
import urllib
import sqlite3

dbname = 'users.db'

def receiveContent(pageNumber):
	url = "https://stackoverflow.com/users?page="+str(pageNumber)+"&tab=reputation&filter=all"
	
	req = urllib.request.Request(url, data=None, 
    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'})
	response = urllib.request.urlopen(req)
	page = response.read().decode('utf-8')
	if response.getcode() == 200:
		return page
	return None

def parsePage(page):
	soup = BeautifulSoup(page, features="html5lib") #html.parser

	userDivs = soup.find_all('div', id='user-browser')[0]
	if len(userDivs) < 1:
		return None
	usersRow = userDivs.find_all('tr')
	
	arr = [] 

	for i in range(0, len(usersRow)):
		usersColumn = usersRow[i].find_all('td')
		for j in range(0, len(usersColumn)):
			user = usersColumn[j].find_all('div')

			userDetails = user[3]
			userTags = user[5].find_all('a') 

			userProfile = userDetails.find_all('a')[0]
			userName = userProfile.get_text()
			userURI = userProfile['href']

			userLocation = userDetails.find_all('span', class_='user-location')[0].get_text();
			userReputation = userDetails.find_all('div')[0].find_all('span', class_='reputation-score')[0].get_text()

			# 0 - Gold, 1 - Silver, 2 - Bronze.
			userBadge = userDetails.find_all('span', class_='badgecount')

			tags = ''
			for a in range(0, len(userTags)):
				if a < len(userTags)-1:
					tags += str(userTags[a].get_text()  + ',')
				else:
					tags += str(userTags[a].get_text())

			arr.append([userName, userURI, userLocation, userReputation, tags])
	return arr

def processUsers(arr):
	conn = sqlite3.connect(dbname)
	for user in arr:
		row = getUser(user[0], conn)
		if len(row) < 1:
			conn.execute('INSERT INTO users(username,url,location,reputation,tags) VALUES (?,?,?,?,?)', (user[0],user[1],user[2],user[3],user[4]))
			conn.commit()
			print (user[0] + " been inserted.")
		else:
			print (str(row[0][1]) + " already exists.")
		#	check values and update if need.
	conn.close()

def getUser(username, conn):
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE username=?', (username,))
    conn.commit()
    rows = cur.fetchall()
    return rows

def installDB():
	sql_create_users_table = """CREATE TABLE IF NOT EXISTS users (
										id integer PRIMARY KEY AUTOINCREMENT,
										username text NOT NULL,
										url text NOT NULL,
										location text NOT NULL,
										reputation text NOT NULL,
										tags text NOT NULL
									);"""
	conn = sqlite3.connect(dbname)
	if conn is not None:
		conn.execute(sql_create_users_table)
	else:
		print("Error! cannot create the database connection.")

installDB()

pageNumber = 10450

while True:
	arr = parsePage(receiveContent(pageNumber))
	if arr is None:
		break
	processUsers(arr)
	usersCount = pageNumber * 36
	print ("******" + str(usersCount) + " users been saved. At page " + str(pageNumber) + "******")
	pageNumber += 1
