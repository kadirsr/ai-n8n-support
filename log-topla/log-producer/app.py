import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler


LOG_FILE = os.getenv("LOG_FILE", "/var/log/market/market-python.ndjson")
LOGS_PER_MINUTE = max(1, int(os.getenv("LOGS_PER_MINUTE", "100")))
SERVICE_NAME = os.getenv("SERVICE_NAME", "market-simulator")
HOST_NAME = os.getenv("HOST_NAME", "market-python-log-producer")
ECS_VERSION = os.getenv("ECS_VERSION", "9.3.0")
SLEEP_SECONDS = 60.0 / LOGS_PER_MINUTE

STORE_IDS = ["store-istanbul-01", "store-ankara-02", "store-izmir-03", "store-bursa-04"]
PAYMENT_METHODS = ["credit_card", "wallet", "meal_card", "bank_transfer"]
PRODUCTS = ["milk", "coffee", "pasta", "rice", "olive_oil", "detergent", "banana", "yogurt"]
CLIENT_IP_POOL = ["192.168.10.14", "192.168.10.22", "192.168.10.38", "10.10.1.44", "10.10.1.57"]

SUCCESS_SCENARIOS = [
    {
        "action": "browse",
        "path": "/catalog",
        "templates": [
            "Customer {customer_id} browsed {product} catalog in {store_id}.",
            "Customer {customer_id} compared {product} prices in {store_id}.",
        ],
    },
    {
        "action": "add_to_cart",
        "path": "/cart",
        "templates": [
            "Customer {customer_id} added {product} to cart in {store_id}.",
            "Customer {customer_id} updated cart with {product} in {store_id}.",
        ],
    },
    {
        "action": "checkout",
        "path": "/checkout",
        "templates": [
            "Customer {customer_id} completed checkout in {store_id} using {payment_method}.",
            "Customer {customer_id} confirmed basket payment in {store_id} using {payment_method}.",
        ],
    },
    {
        "action": "delivery_update",
        "path": "/orders/status",
        "templates": [
            "Customer {customer_id} order moved to courier_assigned for {store_id}.",
            "Customer {customer_id} order status became out_for_delivery in {store_id}.",
        ],
    },
]

FAILURE_SCENARIOS = [
    {
        "action": "payment_failed",
        "path": "/checkout",
        "templates": [
            "Customer {customer_id} payment failed in {store_id} because insufficient_funds.",
            "Customer {customer_id} checkout failed in {store_id} because 3ds_timeout.",
        ],
    },
    {
        "action": "coupon_rejected",
        "path": "/cart/coupon",
        "templates": [
            "Customer {customer_id} coupon validation failed in {store_id}.",
            "Customer {customer_id} campaign code was rejected in {store_id}.",
        ],
    },
    {
        "action": "stock_missing",
        "path": "/cart",
        "templates": [
            "Customer {customer_id} could not add {product}; stock missing in {store_id}.",
            "Customer {customer_id} requested {product} but inventory was empty in {store_id}.",
        ],
    },
    {
        "action": "login_failed",
        "path": "/auth/login",
        "templates": [
            "Customer {customer_id} login failed because invalid_password.",
            "Customer {customer_id} authentication failed because session_expired.",
        ],
    },
]


class ECSJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(record.ecs_document, ensure_ascii=True)


def build_logger() -> logging.Logger:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    logger = logging.getLogger("market_ecs_generator")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    handler.setFormatter(ECSJsonFormatter())
    logger.addHandler(handler)
    return logger


def build_context() -> dict:
    return {
        "customer_id": f"CUST-{random.randint(100000, 999999)}",
        "store_id": random.choice(STORE_IDS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "product": random.choice(PRODUCTS),
        "client_ip": random.choice(CLIENT_IP_POOL),
    }


def choose_scenario() -> tuple[bool, dict]:
    success = random.random() < 0.72
    scenario = random.choice(SUCCESS_SCENARIOS if success else FAILURE_SCENARIOS)
    return success, scenario


def build_event() -> dict:
    success, scenario = choose_scenario()
    context = build_context()
    message = random.choice(scenario["templates"]).format(**context)

    return {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "ecs": {"version": ECS_VERSION},
        "message": message,
        "log": {
            "level": "info" if success else random.choice(["warn", "error"]),
            "logger": "market.simulator",
        },
        "service": {
            "name": SERVICE_NAME,
            "type": "python",
            "version": "1.0.0",
        },
        "event": {
            "module": "market",
            "dataset": "market.application",
            "action": scenario["action"],
            "outcome": "success" if success else "failure",
        },
        "host": {"name": HOST_NAME},
        "user": {"id": context["customer_id"]},
        "transaction": {"id": uuid.uuid4().hex[:16]},
        "client": {"ip": context["client_ip"]},
        "url": {"path": scenario["path"]},
        "labels": {
            "store_id": context["store_id"],
            "payment_method": context["payment_method"],
            "product": context["product"],
        },
    }


def main() -> None:
    logger = build_logger()
    while True:
        ecs_document = build_event()
        logger.info(ecs_document["message"], extra={"ecs_document": ecs_document})
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
