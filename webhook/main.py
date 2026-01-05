from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/events', methods=['POST'])
def webhook_events():
    print("Webhook received!")
    if request.is_json:
        data = request.json
        print(f"Payload: {data}")
        return jsonify({"status": "success", "received": data}), 200
    else:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    # Run on port 5000 by default
    app.run(host='0.0.0.0', port=5000, debug=True)
