import os
import json
from flask import Flask, jsonify, Blueprint, request
from flask_restplus import Api, Resource, fields
from src.utils import load_unspsc_mapping_from_blob

os.environ["ENV_IS_PROD"] = "False"

# Instantiate flask app instance
app = Flask(__name__)

# Instantiate flask_restplus api instance
api = Api(app)

blueprint = Blueprint('api', __name__, url_prefix='/api')

api.init_app(blueprint,
                version='0.0.1',
                title='EasyPayment OPUS MapUNSPSC Api',
                description='Application for mapping GL Account to UNSPSC code',
                contact='DIMA',
                contact_email='elr@kmd.dk; mbb@kmd.dk',
                default_label='EasyPayment OPUS MapUNSPSC')

app.register_blueprint(blueprint)


@api.route("/hello", endpoint='Test Connection')
class HelloWorld(Resource):
    """Endpoint intended to be used for testing that the flask app is running.

    By exposing a simple RESTfull GET endpoint it is possible to test that it 
    is possible to connect to the flask app, and it is running OK.
    """

    def get(self):
        """Endpoint that returns "Hello from DIMA".
        """
        return {'response':'Hello from DIMA'}, 200

datamodel_getunspsc = api.model("Datamodel for getunspsc endpoint", {
    'CLIENT': fields.Integer(description="Control field representing the municipality number", required=True)})

@api.route("/getunspsc", endpoint='Get unspsc')
class GetUNSPSC(Resource):
    """Endpoint intended to be used for getting the mapping between unspsc codes and gl accounts.

    The endpoint is a POST method that has to receive data described in the datamodel for get unspsc.
    """

    @api.expect(datamodel_getunspsc)
    def post(self):
        """Endpoint that returns the UNSPSC - GL Account mapping.
        
        """

        json_data = request.json

        # Convert Client to string
        municipality_number = str(json_data['CLIENT'])

        data_string = load_unspsc_mapping_from_blob(municipality_number)

        return jsonify(json.loads(data_string))


if __name__ == "__main__":
    app.run(port=8000, host="0.0.0.0")