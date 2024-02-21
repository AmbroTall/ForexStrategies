#!/usr/bin/python
# -*- coding: utf-8 -*-
# ib_execution.py
from __future__ import print_function
import datetime
import time

from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, message
from event import FillEvent, OrderEvent
from execution import ExecutionHandler

class IBExecutionHandler(ExecutionHandler):
    """
    Handles order execution via the Interactive Brokers
    API, for use against accounts when trading live
    directly.
    """

    def __init__(self, events, order_routing="SMART", currency="USD"):
        """
        Initializes the IBExecutionHandler instance.

        Parameters:
        events (queue.Queue): Queue to communicate events.
        order_routing (str): Order routing option.
        currency (str): Currency for trading.
        """
        self.events = events
        self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}
        self.tws_conn = self._create_tws_connection()
        self.order_id = self._create_initial_order_id()
        self._register_handlers()

    def _error_handler(self, msg):
        """
        Handles the capturing of error messages.
        """
        # Currently no error handling.
        print("Server Error: %s" % msg)

    def _reply_handler(self, msg):
        """
        Handles server replies.
        """
        if msg.typeName == "openOrder" and msg.orderId == self.order_id and not self.fill_dict.get(msg.orderId):
            self._create_fill_dict_entry(msg)
        elif msg.typeName == "orderStatus" and msg.status == "Filled" and self.fill_dict.get(msg.orderId, {}).get(
                "filled") is False:
            self._create_fill(msg)
        print("Server Response: %s, %s\n" % (msg.typeName, msg))

    def create_tws_connection(self):
        """
        Connects to the Trader Workstation (TWS) running on the
        usual port of 7496, with a clientId of 10.
        The clientId is chosen by us and we will need
        separate IDs for both the execution connection and
        market data connection, if the latter is used elsewhere.
        """
        tws_conn = ibConnection()
        tws_conn.connect()
        return tws_conn

    def create_initial_order_id(self):
        """
        Creates the initial order ID used for Interactive
        Brokers to keep track of submitted orders.
        """
        # There is scope for more logic here, but we
        # will use "1" as the default for now.
        return 1

    def register_handlers(self):
        """
        Register the error and server reply
        message handling functions.
        """
        # Assign the error handling function defined above
        # to the TWS connection
        self.tws_conn.register(self._error_handler, 'Error')
        # Assign all of the server reply messages to the
        # reply_handler function defined above
        self.tws_conn.registerAll(self._reply_handler)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        """
        Creates a Contract object defining what will
        be purchased, at which exchange and in which currency.

        Parameters:
        symbol (str): The ticker symbol for the contract.
        sec_type (str): The security type for the contract ('STK' for stock).
        exch (str): The exchange to carry out the contract on.
        prim_exch (str): The primary exchange to carry out the contract on.
        curr (str): The currency in which to purchase the contract.

        Returns:
        Contract: The Contract object.
        """
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr
        return contract

    def create_order(self, order_type, quantity, action):
        """
        Creates an Order object (Market/Limit) to go long/short.

        Parameters:
        order_type (str): 'MKT' for Market order or 'LMT' for Limit order.
        quantity (int): Integral number of assets to order.
        action (str): 'BUY' for buying or 'SELL' for selling.

        Returns:
        Order: The Order object.
        """
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    def create_fill_dict_entry(self, msg):
        """
        Creates an entry in the Fill Dictionary that lists
        orderIds and provides security information. This is
        needed for the event-driven behaviour of the IB
        server message behaviour.
        """
        self.fill_dict[msg.orderId] = {
            "symbol": msg.contract.m_symbol,
            "exchange": msg.contract.m_exchange,
            "direction": msg.order.m_action,
            "filled": False
        }

    def create_fill(self, msg):
        """
        Handles the creation of the FillEvent that will be
        placed onto the events queue subsequent to an order
        being filled.

        Parameters:
        msg: The message object containing order and contract information.
        """
        fd = self.fill_dict[msg.orderId]
        # Prepare the fill data
        symbol = fd["symbol"]
        exchange = fd["exchange"]
        filled = msg.filled
        direction = fd["direction"]
        fill_cost = msg.avgFillPrice
        # Create a fill event object
        fill_event = FillEvent(
            datetime.datetime.utcnow(), symbol,
            exchange, filled, direction, fill_cost
        )
        # Make sure that multiple messages don’t create
        # additional fills.
        self.fill_dict[msg.orderId]["filled"] = True
        # Place the fill event onto the event queue
        self.events.put(fill_event)

    def execute_order(self, event):
        """
        Creates the necessary InteractiveBrokers order object
        and submits it to IB via their API.
        The results are then queried in order to generate a
        corresponding Fill object, which is placed back on
        the event queue.

        Parameters:
        event: Contains an Event object with order information.
        """
        if event.type == 'ORDER':
            # Prepare the parameters for the asset order
            asset = event.symbol
            asset_type = "STK"
            order_type = event.order_type
            quantity = event.quantity
            direction = event.direction
            # Create the Interactive Brokers contract via the
            # passed Order event
            ib_contract = self._create_contract(
                asset, asset_type, self.order_routing,
                self.order_routing, self.currency
            )
            # Create the Interactive Brokers order via the
            # passed Order event
            ib_order = self._create_order(
                order_type, quantity, direction
            )
            # Use the connection to send the order to IB
            self.tws_conn.placeOrder(
                self.order_id, ib_contract, ib_order
            )
            # NOTE: This following line is crucial.
            # It ensures the order goes through!
            time.sleep(1)
            # Increment the order ID for this session
            self.order_id += 1
