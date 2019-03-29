#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import json
from flask import Flask, request, make_response, jsonify

app = Flask(__name__)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(name)s][%(levelname)s][%(message)s]')
logger = logging.getLogger(__name__)

'''
TODO: RESTful API Entry for data analysis platform
'''

@app.route("/")
def hello():
    return "Hello, world!"


if __name__ == "__main__":
    app.run(debug=True)