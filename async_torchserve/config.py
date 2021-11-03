config = {
    "bootstrap_servers": 'localhost:9092',
    "async_produce": False,
    "models": [
        {
            "module_name": "image_classification.predict",
            "class_name": "ModelPredictor",
        }
    ]
}