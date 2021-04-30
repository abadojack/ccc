import time
import datetime
import sys
import multiprocessing
import json
from concurrent import futures
import grpc
import bank_pb2
import bank_pb2_grpc


class Branch(bank_pb2_grpc.BankServicer):
    """ Branch class definition """

    def __init__(self, ID, balance, branches):
        # unique ID of the Branch
        self.id = ID
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # Binded address
        self.bind_address = str
        # the list of Branches including IDs and Addresses
        self.branchList = list()
        # local logical clock connected to logical clock
        self.local_clock = 0
        # a list of events connected to logical clock
        self.clock_events = None
        # logical clock output file
        self.clock_output = None
        # list of writesets
        self.writeSets = list()

    def MsgDelivery(self, request, context):
        self.recvMsg.append(request)

        balance_result = None
        response_result = None

        # monotonic writes
        if (request.destination_id != -1) or (request.Operation == bank_pb2.QUERY):
            while not(self.IsLastExecuted(request.source_id, request.progressive_id)):
                self.Event_Execute()

        # query
        if request.request_type == bank_pb2.QUERY:
            if (self.clock_events != None):
                self.Event_Request(request.clock)
                self.Event_Execute()
            response_result, balance_result = self.Query()

        # deposit
        if request.request_type == bank_pb2.DEPOSIT:
            if request.branch_client_id == -1:
                if (self.clock_events != None):
                    self.Propagate_Response(request.clock)
                    self.register_event(request.request_id, "propagate_deposit_request")
                    self.Event_Execute()
                    self.register_event(request.request_id, "propagate_deposit_execute")
            else:
                if (self.clock_events != None):
                    self.Event_Request(request.clock)
                    self.register_event(request.request_id, "deposit_request")
                    self.Event_Execute()
                    self.register_event(request.request_id, "deposit_execute")
            response_result, balance_result = self.Deposit(request.Amount)

        # withdraw
        if request.request_type == bank_pb2.WITHDRAW:
            if request.branch_client_id == -1:
                if (self.clock_events != None):
                    self.Propagate_Response(request.clock)
                    self.register_event(request.request_id, "propagate_withdraw_request")
                    self.Event_Execute()
                    self.register_event(request.request_id, "propagate_withdraw_execute")
            else:
                if (self.clock_events != None):
                    self.Event_Request(request.clock)
                    self.register_event(request.request_id, "withdraw_request")
                    self.Event_Execute()
                    self.register_event(request.request_id, "withdraw_execute")
            response_result, balance_result = self.Withdraw(request.money)

        if request.branch_client_id == -1:
            if (self.clock_events != None):
                self.Propagate_Response(request.clock)
        else:
            if request.request_type == bank_pb2.DEPOSIT:
                if (self.clock_events != None):
                    self.Propagate_Request()
                self.Propagate_Deposit(request.request_id, request.money)
            if request.OP == bank_pb2.WITHDRAW:
                if (self.clock_events != None):
                    self.Propagate_Request()
                self.Propagate_Withdraw(request.request_id, request.money)

        self.Event_Response()

        response = bank_pb2.MsgDeliveryResponse(
            request_id=request.REQ_ID,
            status_code=response_result,
            money=balance_result,
            clock=self.local_clock
        )

        if (self.clock_events != None):
            if request.request_type == bank_pb2.DEPOSIT:
                if request.branch_client_id == -1:
                    self.register_event(request.request_id, "deposit_broadcast_response")
                else:
                    self.register_event(request.request_id, "deposit_response")
            if request.request_type == bank_pb2.WITHDRAW:
                if request.branch_client_id == -1:
                    self.register_event(request.request_id, "withdraw_broadcast_response")
                else:
                    self.register_event(request.request_id, "withdraw_response")

        return response

    def ReserveWriteSet(self, request):
        return_clock = 0
        if (self.clock_events != None):             
            self.Event_Request(request.clock)
            return_clock = self.local_clock

        max_wid = request.requested_id
        for curr_set in self.writeSets:
            if curr_set.Customer == request.source_id:
                if curr_set.progressive_id > max_wid:
                    max_wid = curr_set.progressive_id

        new_id = max_wid+1

        self.writeSets.append(WriteSet(request.source_id, new_id, False))

        return bank_pb2.WriteSetResponse(
            clock       = return_clock,
            source_id        = request.source_id,
            progressive_id     = new_id
        )

    def IsLastWriteSet(self, request):
        last_id = request.requested_id

        for curr_set in self.writeSets:
            if (curr_set.Customer == request.source_id) and not(curr_set.isExecuted):
                if curr_set.progressive_id < last_id:
                    last_id = curr_set.progressive_id

        return bank_pb2.IsLastResponse(
            is_last=bool(last_id == request.requested_id)
        )

    def IsLastExecuted(self, customer_id):
        return_value = False
        for curr_branch in self.branchList:
            if self.id != curr_branch[0]:
                try:
                    msgStub = bank_pb2_grpc.BankStub(grpc.insecure_channel(curr_branch[1]))

                    response = msgStub.IsLastWriteSet(
                        bank_pb2.WriteSetRequest(
                            source_id=customer_id,               
                            progressive_id=request_id,             
                            clock=self.local_clock
                        )
                    )
                    
                    if response.is_last:
                        return_value = True
                        break

                except grpc.RpcError as error:
                    print(error)

        return return_value


    def Query(self):
        return bank_pb2.SUCCESS, self.balance

    def Deposit(self, amount):
        validate_amount(amount)

        self.balance = self.balance + amount
        return bank_pb2.SUCCESS, self.balance

    def Withdraw(self, amount):
        validate_amount(amount)

        b = self.balance - amount
        if b < 0:
            return bank_pb2.FAILURE, amount
        self.balance = b
        return bank_pb2.SUCCESS, b

    def Propagate_Deposit(self, request_id, amount):
        for stub in self.branchList:
            if self.id != stub[0]:
                try:
                    msgStub = bank_pb2_grpc.BankStub(grpc.insecure_channel(stub[1]))

                    if (self.clock_events != None):
                        response = msgStub.MsgDelivery(
                            bank_pb2.MsgDeliveryRequest(
                                money=amount,
                                request_type=bank_pb2.DEPOSIT,
                                request_id=request_id,
                                branch_client_id=-1, # do not propagate deposit
                                clock=self.local_clock
                            )
                        )
                    else:
                        response = msgStub.MsgDelivery(
                            bank_pb2.MsgDeliveryRequest(
                                request_type=bank_pb2.DEPOSIT,
                                money=amount,
                                request_id=request_id,
                                branch_client_id=-1
                            )
                        )

                    self.Event_Response()

                except grpc.RpcError as rpc_error_call:
                    pass


    def Propagate_Withdraw(self, request_id, amount):
        for stub in self.branchList:
            if self.id != stub[0]:
                try:
                    msgStub = bank_pb2_grpc.BankStub(grpc.insecure_channel(stub[1]))

                    if (self.clock_events != None):
                        response = msgStub.MsgDelivery(
                            bank_pb2.MsgDeliveryRequest(
                                money=amount,
                                request_type=bank_pb2.WITHDRAW,
                                request_id=request_id,
                                branch_client_id=-1, # do not propagate withdraw
                                clock=self.local_clock
                            )
                        )
                    else:
                        response = msgStub.MsgDelivery(
                            bank_pb2.MsgDeliveryRequest(
                                request_type=bank_pb2.WITHDRAW,
                                money=amount,
                                request_id=request_id,
                                branch_client_id=-1
                            )
                        )

                    self.Event_Response()

                except grpc.RpcError as rpc_error_call:
                    pass

    def Event_Request(self, clock):
        self.local_clock = max(self.local_clock, clock) + 1

    def Event_Execute(self):
        self.local_clock += 1

    def Propagate_Request(self):
        self.local_clock += 1

    def Propagate_Response(self, clock):
        self.local_clock = max(self.local_clock, clock) + 1

    def Propagate_Execute(self):
        self.local_clock += 1

    def Event_Response(self):
        self.local_clock += 1

    def register_event(self, id, name):
        if (self.clock_events != None):
            self.clock_events.append({'id': id, 'name': name, 'clock': self.local_clock})

    def validate_amount(amount):
        if amount <= 0:
            return bank_pb2.ERROR, self.balance


def run_branch(Branch, clock_file):
    options = (('grpc.so_reuseport', 1),)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1,), options=options)
    bank_pb2_grpc.add_BankServicer_to_server(Branch, server)

    server.add_insecure_port(Branch.bind_address)
    server.start()
