import unittest
import json
from src.routes import app
from src.utils import load_unspsc_mapping_from_blob

class TestRoutes(unittest.TestCase):

    def setUp(self):
        app.testing = True
        self.app = app.test_client()

    def test_hello(self):
        result = self.app.get('/api/hello')
        self.assertEqual(result.json['response'], 'Hello from DIMA')
    
    def test_getunspsc(self):

        client = 123

        result = self.app.post('/api/getunspsc', 
                               headers={'Content-Type': 'application/json'},
                               data=json.dumps({'CLIENT': client}))

        self.assertEqual(result.json, load_unspsc_mapping_from_blob(client))