import time
import datetime
import multiprocessing
import json
from concurrent import futures
import grpc
import bank_pb2
import bank_pb2_grpc   

operation = {
    "query": bank_pb2.QUERY,
    "deposit": bank_pb2.DEPOSIT,
    "withdraw": bank_pb2.WITHDRAW
}

client_lock = multiprocessing.Lock()

class Customer:
    def __init__(self, _id, events):
        # unique ID of the Customer
        self.id = _id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None
        # list of writesets
        self.writeSets = list()
        # list of branches
        self.branches = list()

    def createStub(self, Branch_address):
        self.stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(Branch_address))

        client = grpc.server(futures.ThreadPoolExecutor(max_workers=1,),)
        client.start()

    def executeEvents(self, output_file):      
        record = {'id': self.id, 'recv': []}
        for event in self.events:
            request_id = event['id']
            request_operation = operation[event['interface']]
            request_amount = event['money']

            try:
                request_destination = event['dest']
            except KeyError:
                request_destination = self.id

            try:
                if request.operation == bank_pb2.QUERY:
                    request_amount = ""
                else:
                    request_amount = str(event['money'])

                msgStub = None
                for curr_branch in self.branches:
                    if request_dest == curr_branch[0]:
                        msgStub = bank_pb2_grpc.BankStub(grpc.insecure_channel(curr_branch[1]))
                        break
                
                if msgStub != None:
                    progressive_request_id = request_id
                    if request_operation != bank_pb2.QUERY:
                        writeset_response = msgStub.ReserveWriteSet(
                            bank_pb2.WriteSetRequest(
                                source_id=self.id,
                                progressive_id=request_id,
                                clock=0
                            )
                        )

                    self.writeSets.append(
                        bank_pb2.WriteSet(
                            source_id = self.id,
                            progressive_id = writeset_response.progressive_id, 
                            is_executed = False)
                    )
                    progressive_request_id = writeset_response.progressive_id


                response = self.stub.MsgDelivery(
                    bank_pb2.MsgDeliveryRequest(
                        request_id=request_id,
                        request_type=request_operation,
                        money=request_amount,
                        source_id=self.id,
                        clock=0,
                        progressive_id=progressive_request_id,
                        destination_id=request_destination,
                        source=bank_pb2.CUSTOMER
                    )
                )



                if request_operation == bank_pb2.QUERY:
                    values['money'] = response.money
                record['recv'].append(values)
                                 
                if record['recv']:
                    with open(f'{output_file}', 'a') as outfile:
                        json.dump(record, outfile)
                        outfile.write('\n')
                        
            except grpc.RpcError as rpc_error_call:
                pass


    def run_customer(self, Branch_address, output_file):
        Customer.createStub(self, Branch_address)
        Customer.executeEvents(self, output_file)
