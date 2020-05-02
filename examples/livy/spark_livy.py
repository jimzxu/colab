import requests

import kerberos
import requests_kerberos
import json, pprint, textwrap

host = 'http://ip-172-31-60-124.ec2.internal:8998'
auth = requests_kerberos.HTTPKerberosAuth(mutual_authentication=requests_kerberos.REQUIRED, force_preemptive=True)

data = {'kind': 'pyspark'}
headers = {'Content-Type': 'application/json'}

r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers, auth=auth)
session_url = host + r.headers['location']

data = {'code': '1 + 1'}

data = {
  'code': textwrap.dedent("""
	import random
	NUM_SAMPLES = 100000
	def sample(p):
	  x, y = random.random(), random.random()
	  return 1 if x-x + y-y < 1 else 0

	count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
	print "Pi is roughly %f" % (4.0 - count / NUM_SAMPLES)
	""")
}

data = {
  'code': textwrap.dedent("""
	import sys
	rdd = sc.parallelize(range(100), 10)
	print(rdd.collect())
	""")
}
statements_url = session_url + '/statements'

r = requests.post(statements_url, data=json.dumps(data), headers=headers, auth=auth)
pprint.pprint(r.json())

r = requests.get(statements_url, headers=headers, auth=auth)
r.json()

r = requests.delete(session_url, headers=headers, auth=auth)