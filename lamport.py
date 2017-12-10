import threading
import time
import Queue as queue
import socket,pickle
import sys,select
import pdb

totalOrderedQueue = queue.PriorityQueue()
requestQueue = queue.Queue()
replyQueue = queue.Queue()
releaseQueue = queue.Queue()
selfProcessQueue = queue.Queue()
exitQueue = queue.Queue()

name_info = {}
port_info = {}
ref_client_info = {}
port2client = {}
client2name = {}


class Message():
	def __init__(self,messageType,lamportTime,numLikes,increment,senderInfo):
		self.messageType = messageType
		self.lamportTime = lamportTime
		self.numLikes = numLikes
		self.senderInfo = senderInfo
		self.increment = increment
	def getMessageType(self):
		return self.messageType
	def getLamportTime(self):
		return self.lamportTime
	def getNumLikes(self):
		return self.numLikes
	def getSenderInfo(self):
		return self.senderInfo
	def getIncrement(self):
		return self.increment
	## comparator for priority queue
	def __cmp__(self,other):
		if(self.lamportTime < other.lamportTime):
			return -1
		elif(self.lamportTime == other.lamportTime):
			if(self.senderInfo.lower()[0] < other.senderInfo.lower()[0]):
				return -1
			else:
				return +1
		else:
			return +1

def parse_file(filename,processName):
	f = open(filename,'r')
	count = 1
	for line in f:
		a = line.split()
		if(a[0] == processName):
			name_info['server'] = processName
			port_info['server'] = int(a[1])
		else:
			name_info['client'+str(count)] = a[0]
			port_info['client'+str(count)] = int(a[1])
			count = count + 1
	## To get the information about the client if we know the port number	
	for key,value in port_info.items():
		port2client[str(value)] = key
	for key,value in name_info.items():
		client2name[str(value)] = key

def readPost(filename='post_file.txt'):
	f = open(filename,'r')
	lines = f.readlines()
	post = lines[0]
	likes = lines[1]
	print "Post : " + str(post)
	return [post,likes]

def check_dict(currDict):
	for key,value in currDict.items():
		if value < 1:
			return False
	return True
		

## Console thread only to get the information from console
class console_thread(threading.Thread):
	def __init__(self,name,selfProcessQueueLock):
		threading.Thread.__init__(self)
		self.name = name
		self.selfProcessQueueLock = selfProcessQueueLock
	
	def run(self):
		while(True):
			while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
  				line = sys.stdin.readline().strip()
  				if (line.split()[0] == "Like"):
					if(len(line.split()) == 2):
						selfProcessQueue.put(int(line.split()[1]))
					else:
						selfProcessQueue.put(1) 
				## To Quit the System
				elif (line.split()[0]=="Quit"):
					exitQueue.put(line)
					break
				## To Read the post
				elif (line.split()[0]=="Read"):
					readPost()
				else:
					print (self.name).upper() + ": Invalid input"	
			if (not exitQueue.empty()):
				break	


class server_thread(threading.Thread):
	def __init__(self,name,port,lamportTime,numLikes,post,selfProcessQueueLock, requestQueueLock,totalOrderedQueueLock,replyQueueLock,releaseQueueLock):
		threading.Thread.__init__(self)
		self.name = name
		self.port = port
		self.currLamportTime = lamportTime
		self.numLikes = numLikes
		self.post = post
		self.requestQueueLock = requestQueueLock
		self.totalOrderedQueueLock = totalOrderedQueueLock
		self.replyQueueLock = replyQueueLock
		self.releaseQueueLock = releaseQueueLock
		self.selfProcessQueueLock = selfProcessQueueLock
		self.replyDict = {'client1':0,'client2':0,'client3':0}
		self.selfMessageQueue = queue.Queue()	
		
	def run(self):

		## Invoke a server with the following port  and wait for all the connections to this server from other process
		self.invoke_server()
		## Once all are connections made then the process waits for the input from console and sends it to the other process
		self.send_info()
		print "Total number of likes for the post is " + str(self.numLikes) + ". Process is exiting"
		## close the server socket at the process
		self.server_socket.close()
		

	## Creating a server socket and waiting for the other clients to connectto it
	def invoke_server(self):
		server_socket = socket.socket()
		self.server_socket = server_socket
		server_socket.bind(('',self.port))
		server_socket.listen(5)
		
		self.client1,self.addr1 = server_socket.accept()
		self.client1_info = self.client1.recv(1024)
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client1_info]] + " has been formed."
		
		self.client2,self.addr2 = server_socket.accept()
		self.client2_info = self.client2.recv(1024)
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client2_info]] + " has been formed."

		self.client3,self.addr3 = server_socket.accept()
		self.client3_info = self.client3.recv(1024)
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client3_info]] + " has been formed."

		self.assign_clients()


	def assign_clients(self):
		ref_client_info[str(port2client[self.client1_info])] = self.client1
		ref_client_info[str(port2client[self.client2_info])] = self.client2
		ref_client_info[str(port2client[self.client3_info])] = self.client3		
		

	def send_info(self):
		count = 0
		while(True):
			if (not exitQueue.empty()):
				lamportTime = self.currLamportTime
				numLikes = self.numLikes
				ref_client_info['client1'].send(pickle.dumps(Message("QUIT",lamportTime,numLikes,0,self.name)))
				ref_client_info['client2'].send(pickle.dumps(Message("QUIT",lamportTime,numLikes,0,self.name)))
				ref_client_info['client3'].send(pickle.dumps(Message("QUIT",lamportTime,numLikes,0,self.name)))
				break
			while(not selfProcessQueue.empty()):
				increment = selfProcessQueue.get()
				self.selfMessageQueue.put(Message("Request",self.currLamportTime,self.numLikes,increment,self.name))
				
				
			## if there is a previous request then it waits for 5s and then sends it to the other process
			while(not self.selfMessageQueue.empty()):
				time.sleep(5)
				incomingMessage = self.selfMessageQueue.get()
				increment = incomingMessage.getIncrement()
				self.currLamportTime= max(self.currLamportTime,	incomingMessage.getLamportTime()) + 1	
				totalOrderedQueue.put(Message("Request",self.currLamportTime,self.numLikes,increment,self.name))
				## Sending the requests to all the process when Liked after a delay of 5s
				ref_client_info['client1'].send(pickle.dumps(Message("REQUEST",self.currLamportTime,self.numLikes,increment,self.name)))
				print (self.name).upper()+ ": Request sent to " + name_info['client1'] + " Time : " + str(self.currLamportTime )
				ref_client_info['client2'].send(pickle.dumps(Message("REQUEST",self.currLamportTime,self.numLikes,increment,self.name)))
				print (self.name).upper()+ ": Request sent to " + name_info['client2'] + " Time : " + str(self.currLamportTime )
				ref_client_info['client3'].send(pickle.dumps(Message("REQUEST",self.currLamportTime,self.numLikes,increment,self.name)))
				print (self.name).upper()+ ": Request sent to " + name_info['client3'] + " Time : " + str(self.currLamportTime )
					
			## for the incoming requests from other process you need to send a reply to them.
			while(not requestQueue.empty()):
				self.requestQueueLock.acquire()
				incomingMessage = requestQueue.get()
				self.requestQueueLock.release()
				incomingLamportTime = incomingMessage.getLamportTime()
				incomingSender = incomingMessage.getSenderInfo()
				self.currLamportTime = max(incomingLamportTime,self.currLamportTime) + 1
				print (self.name).upper()+ ": Request received from " +incomingSender + " Time : " + str(self.currLamportTime) 
				## from clients name we know to which socket is it connected
				self.currLamportTime += 1
				ref_client_info[client2name[incomingSender]].send(pickle.dumps(Message("REPLY",self.currLamportTime,self.numLikes,0,self.name)))
				print (self.name).upper()+ ": Reply sent to " +incomingSender + " Time : " + str(self.currLamportTime) 


			## The replies from other proess for the request sent by you, needs to be processed before checking the total order of the priority queue

			while(not replyQueue.empty()):
				self.replyQueueLock.acquire()
				incomingMessage = replyQueue.get()
				self.replyQueueLock.release()
				incomingLamportTime = incomingMessage.getLamportTime()
				incomingSender = incomingMessage.getSenderInfo()
				self.currLamportTime = max(self.currLamportTime,incomingLamportTime) + 1
				self.replyDict[client2name[incomingSender]] += 1
				print (self.name).upper()+ ": Reply received from "+incomingSender+ " Time : "+ str(self.currLamportTime)


			## You will either send a release message or check for the release message to remove the top element from total Order Queue
			while((not releaseQueue.empty()) or (check_dict(self.replyDict))):
				totalOrderedMessage = totalOrderedQueue.queue[0]
				## if the top element corresponds to the process itself it should send a release message with numlikes
				if((check_dict(self.replyDict))):
					if((totalOrderedMessage.getSenderInfo() == self.name)):
						self.totalOrderedQueueLock.acquire()
						message = totalOrderedQueue.get()
						self.totalOrderedQueueLock.release()
						self.currLamportTime += 1
						self.numLikes = self.numLikes + message.getIncrement()						
						## Sending release message to the other processes
						ref_client_info['client1'].send(pickle.dumps(Message("RELEASE",self.currLamportTime,self.numLikes,0,self.name)))
						print (self.name).upper()+ ": Release sent to " + name_info['client1'] + " Time : " + str(self.currLamportTime) 

						ref_client_info['client2'].send(pickle.dumps(Message("RELEASE",self.currLamportTime,self.numLikes,0,self.name)))
						print (self.name).upper()+ ": Release sent to " + name_info['client2'] + " Time : " + str(self.currLamportTime)

						ref_client_info['client3'].send(pickle.dumps(Message("RELEASE",self.currLamportTime,self.numLikes,0,self.name)))
						print (self.name).upper()+ ": Release sent to " + name_info['client3'] + " Time : " + str(self.currLamportTime)						
						
						print (self.name).upper() + ": Total number of likes is " + str(self.numLikes)
						self.replyDict['client1'] -= 1
						self.replyDict['client2'] -= 1
						self.replyDict['client3'] -= 1
						time.sleep(1)

				if(not releaseQueue.empty()):
					topReleaseMessage = releaseQueue.queue[0]
					## if the release is received from other process then remove the release queue element
					## and elements from top Ordered queue
					###print "Release Queue not empty"
					## print 	totalOrderedMessage.getSenderInfo(), topReleaseMessage.getSenderInfo()
					if((totalOrderedMessage.getSenderInfo() == topReleaseMessage.getSenderInfo())):
						self.currLamportTime = max(self.currLamportTime,topReleaseMessage.getLamportTime()) + 1
						self.numLikes = topReleaseMessage.getNumLikes()
						print (self.name).upper()+ ": Release obtained from " + totalOrderedMessage.getSenderInfo() +  " Time : " + str(self.currLamportTime) + " Likes : " + str(self.numLikes)
						self.releaseQueueLock.acquire()
						releaseQueue.get()
						self.releaseQueueLock.release()
						self.totalOrderedQueueLock.acquire()
						totalOrderedQueue.get()
						self.totalOrderedQueueLock.release()
						time.sleep(1)
					


class client_thread(threading.Thread):
	def __init__(self,name,port,requestQueueLock,totalOrderedQueueLock,replyQueueLock,releaseQueueLock):
		threading.Thread.__init__(self)
		self.name = name
		self.port = port
		self.requestQueueLock = requestQueueLock
		self.totalOrderedQueueLock = totalOrderedQueueLock
		self.replyQueueLock = replyQueueLock
		self.releaseQueueLock = releaseQueueLock

	def run(self):
		self.invoke_client()
		self.get_info()
		self.client_socket.close()		

	def invoke_client(self):
		client_socket = socket.socket()
		while (True):
			try:
				client_socket.connect(('127.0.0.1',self.port))
				client_socket.send(str(port_info['server']))
				break
			except socket.error as msg:
				continue
		self.client_socket = client_socket


	def get_info(self):
		while True:
			recvd = self.client_socket.recv(1024)
			recvdMessage = pickle.loads(recvd)
			if (recvdMessage.getMessageType()=="QUIT"):
				exitQueue.put("Quit")				
				break
			## Client server has received a message which is of type REQUEST.
			## Insert them into request queue so the server thread can send a reply to the process
			## Insert into the total Order Queue so the server thread decides who to get the priority
			if(recvdMessage.getMessageType() == "REQUEST"):
				###print name_info['server'].upper() + ": Request has been received from " + recvdMessage.getSenderInfo()
				self.requestQueueLock.acquire()
				self.totalOrderedQueueLock.acquire()
				requestQueue.put(recvdMessage)
				totalOrderedQueue.put(recvdMessage)
				self.requestQueueLock.release()
				self.totalOrderedQueueLock.release()

			## if the received message is a reply then put them in the reply queue so the 
			## sever thread knows of it.

			if(recvdMessage.getMessageType() == "REPLY"):
				self.replyQueueLock.acquire()
				replyQueue.put(recvdMessage)
				self.replyQueueLock.release()
				

			## if you get a release message then put the message in release queue the server
			## then pops the top element from totally ordered queue

			if(recvdMessage.getMessageType() == "RELEASE"):
				self.releaseQueueLock.acquire()
				releaseQueue.put(recvdMessage)
				self.releaseQueueLock.release()						


def process(argv):
	
	parse_file(sys.argv[2],sys.argv[1])
	
	totalOrderedQueueLock = threading.RLock()
	replyQueueLock = threading.RLock()
	releaseQueueLock = threading.RLock()
	requestQueueLock = threading.RLock()
	selfProcessQueueLock = threading.RLock()
	
	console = console_thread(name_info['server'],selfProcessQueueLock)
	print name_info['server'].upper() + ": Reading the post"
	inputList = readPost(sys.argv[3])
	print "Initial Number of Likes : " + str(inputList[1])
	server  = server_thread(name_info['server'],port_info['server'],0,int(inputList[1]),str(inputList[1]),selfProcessQueueLock,requestQueueLock,totalOrderedQueueLock, replyQueueLock,releaseQueueLock)
	client1 = client_thread(name_info['client1'],port_info['client1'],requestQueueLock,totalOrderedQueueLock, replyQueueLock, releaseQueueLock)
	client2 = client_thread(name_info['client2'],port_info['client2'],requestQueueLock,totalOrderedQueueLock, replyQueueLock, releaseQueueLock)
	client3 = client_thread(name_info['client3'],port_info['client3'],requestQueueLock, totalOrderedQueueLock, replyQueueLock, releaseQueueLock)

	server.start()
	client1.start()
	client2.start()
	client3.start()
	console.start()


if __name__ == '__main__':
	##pdb.set_trace()
	process(sys.argv)
