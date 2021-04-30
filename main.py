import json 
import multiprocessing
import tempfile


from Customer import Customer
from Branch import Branch, run_branch

branches = list()
customers = list()

def load_input_file(file):
    try:
        file_str = open(file)
    except Exception as e:
        raise e

    json_file = json.load(file_str)

    for item in json_file:
        if item['type'] == 'branch':
            branch = Branch(item['id'], item['balance'], list())
            branches.append(branch)
        elif item['type'] == 'customer':
            events = list()
            for event in item['events']:
                events.append(event)
            customer = Customer(item['id'], events)
            customers.append(customer)

    for branch in branches:
        for b in branches:
            if branch.id != b.id:
                branch.branches.append(b.id)

    for customer in customers:
        for e in customer.events:
            print(e)
            #customer.events.append(e)

    file_str.close()

if __name__ == '__main__':
    input_file = 'input.json'
    output_file = 'output.json'
    clock_file = 'clock.json'

    load_input_file(input_file)

    port = 60000

    branches_addresses_ids = []

    for branch in branches:
        print(f'Branch {branch.id} bind address')
        branch.bind_address = '[::]:{}'.format(port+1)

        if not clock_file:
            branch.clock_events = None
            branch.clock_output = None
        else:
            branch.clock_events = list()
            branch.clock_output = tempfile.NamedTemporaryFile(mode='w+', delete = False)
            branch.clock_output.write('\n')
            branch.clock_output.close()

        branches_addresses_ids.append([branch.id, branch.bind_address])


    for branch in branches:
        branch.branchList = branches_addresses_ids[:]

    for branch in branches:
        print(f'Branch-{branch.id}')
        worker = multiprocessing.Process(name=f'Branch-{branch.id}', target=run_branch,
                                            args=(branch,clock_file,1))
        worker.start()
        workers.append(worker)

    for customer in customers:
        for i in range(len(branches_addresses_ids)):
            if branches_addresses_ids[i][0] == curr_customer.id:
                Branch_address = branches_addresses_ids [i][1]
                break
        
        worker = multiprocessing.Process(name=f'Customer-{customer.id}', target=Customer.run_customer,
                                            args=(customer,Branch_address,output_file,1))
        worker.start()
        workers.append(worker)


    try:
        for worker in workers:
            worker.join()
    except Exception as e:
        print(e)

    # if clock_file:
    #     records = []
    #     total_records = []
    #     for branch in branches:
    #         if branch.clock_output:
    #             with open(f'{branch.clock_output.name}', 'r') as infile:
    #                 records = json.load(infile)
    #                 total_records.append(records)
    #             os.remove(curr_branch.clock_output.name)
    #     with open(f'{clock_file}', 'w+') as outfile:
    #         if (PRETTY_JSON):
    #             json.dump(total_records, outfile, indent=2)
    #         else:
    #             json.dump(total_records, outfile)
    #         outfile.write('\n')
    #         # Writes events in output file ordered by event ID/clock
    #         events = []
    #         for curr_record in total_records:
    #             for event in curr_record['data']:
    #                 events.append(event)
    #         events.sort(key=lambda x: x['clock'])
    #         events.sort(key=lambda x: x['id'])
    #         # Probably not very Pythonian, but I have given my best :-P (and it works)
    #         curr_event_id = -1
    #         curr_new_event = -1
    #         new_events = []
    #         for curr_record in events:
    #             if curr_record['id'] != curr_event_id:
    #                 curr_event_id = curr_record['id']
    #                 curr_new_event += 1
    #                 new_events.append("eventid:")
    #                 new_events.append(curr_event_id)
    #                 new_events.append("data")
    #             new_events.append(
    #             {
    #                 'clock': curr_record['clock'],
    #                 'name': curr_record['name'],
    #             })
    #         # Dumps the vent into the JSON file, one Event ID at the time
    #         curr_event_id = -1
    #         curr_new_event = -1
    #         event_dict = {"eventid": int}
    #         for curr_record in events:
    #             if curr_record['id'] != curr_event_id:
    #                 if (PRETTY_JSON):
    #                     if (curr_event_id >= 0):
    #                         json.dump(event_dict, outfile, indent=2)
    #                 else:
    #                     if (curr_event_id >= 0):
    #                         json.dump(event_dict, outfile)
    #                 curr_event_id = curr_record['id']
    #                 curr_new_event += 1
    #                 event_dict ["eventid"] = curr_event_id
    #                 event_dict ["data"] = []
    #             event_dict ["data"].append(
    #             {
    #                 'clock': curr_record['clock'],
    #                 'name': curr_record['name'],
    #             })

    #         if (PRETTY_JSON):
    #             if any((event_dict.get('data'))):
    #                 json.dump(event_dict, outfile, indent=2)
    #         else:
    #             if any((event_dict.get('data'))):
    #                 json.dump(event_dict, outfile)

    #         outfile.close()

    # # Just in case
    # for worker in workers:
    #     worker.terminate()



