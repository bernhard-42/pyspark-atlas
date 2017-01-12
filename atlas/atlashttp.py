import requests

class AtlasHttp(object):

    def __init__(self, host, user, password):
      self.baseUrl = baseUrl = "http://%s/api/atlas" % host
      self.user = user
      self.password = password
      self.auth = (user, password)


    def GET(self, path):
        r = requests.get("%s/%s" % (self.baseUrl, path), auth=self.auth)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 404:
            return None
        else:
            raise Exception("Status Code: %d\n%s" %(r.status_code, r.text))


    def POST(self, path, content):
        r = requests.post("%s/%s" % (self.baseUrl, path), json=content, auth=self.auth, \
              headers={"Content-Type": "application/json;charset=UTF-8"}
            )
        if r.status_code in [200, 201]:
            return r.json()
        else:
            raise Exception("Status Code: %d\n%s" %(r.status_code, r.text))


    def DELETE(self, path):
        r = requests.delete("%s/%s" % (self.baseUrl, path), auth=self.auth)
        if r.status_code == 200:
            return r.text
        else:
            raise Exception("Status Code: %d\n%s" %(r.status_code, r.text))
