import boto3


### Functionalities ###
# The purpose of this class is to:
# 1. Check a queue that will contain queries for the controller and receive all messages in that queue
# 2. Query all the VASSAR containiers that are currently running
# 3. Start VASSAR containers and create queues for them
# 4. Stop VASSAR containers and remove their respective queues

class AWS_Interface:

    def __init__(self, controller_queue_url):
        self.controller_queue_url = controller_queue_url
        self.sqs = boto3.resource('sqs')
        self.ecs = boto3.client('ecs')
        self.queueCount = 0
        self.queueDict = {}

        # Not implemented -----> Dictionary: problem --> [ (queueName, task), (queueName, task), ... ]
        # Dictionary: problem --> [ queueName, queueName, ... ]
        self.problem_to_instance_dict = {}

    # --> If no queue name is provided, check the query queue
    # --> If a queue name is provided, check that queue for messages
    # --> Return the problem type
    def receive_message(self, queue_name=None, num_messages=1):
        if queue_name is None:
            response = self.sqs.receive_message(QueueUrl=self.controller_queue_url, MaxNumberOfMessages=num_messages)
        else:
            response = self.sqs.receive_message(QueueUrl=queue_name, MaxNumberOfMessages=num_messages)
        return response


    # --> Delete a message from the queue
    def delete_message(self, receipt_handle, queue_name=None):
        if queue_name is None:
            self.sqs.delete_message(QueueUrl=self.controller_queue_url, ReceiptHandle=receipt_handle)
        else:
            self.sqs.delete_message(QueueUrl=queue_name, ReceiptHandle=receipt_handle)

        return 0


    # --> String queue_url
    # --> String message_body
    def send_message(self, queue_url, message_body):
        response = self.sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
        return response





    def start_task(self, task_definition, new_queue_name):
        response = self.ecs.run_task(taskDefinition=task_definition,
                                     overrides={
                                         'containerOverrides': [
                                             {
                                                 'environment': [
                                                     {
                                                         'QUEUE': new_queue_name,
                                                     },
                                                 ],
                                             },
                                         ]
                                     })
        return response

    def stop_task(self):
        return 0

    # --> Creates a queue named on the queue count
    # --> When the queue is created, increment the queue count so the next queue is named something different
    def create_queue(self):
        queue_name = str(self.queueCount)
        queue = self.sqs.create_queue(QueueName=queue_name)
        self.queueDict[queue_name] = queue
        self.queueCount = self.queueCount + 1
        return queue_name




    # --> Checks to see if a VASSAR instance is already set up for the problem
    # --> If a VASSAR instance is already set up and has less than 5 messages in the queue, return the name of that queue
    # --> If a VASSAR instance is not set up with that problem type or has more than 5 messages in the queue --> create a new VASSAR instace --> create a new queue --> return the queue
    def get_problem_queue(self, problem):
        # Check to see if this problem has already been setup -- if yes, return the queue if there are less than 5 messages
        if problem in self.problem_to_instance_dict:
            for queue_name in self.problem_to_instance_dict[problem]:
                num_messages = self.messages_in_queue(queue_name)
                if num_messages < 5:
                    return queue_name

        # --> If this part of the function is reached -- create a new queue -- start a new task for the problem -- return the queue
        new_queue_name = self.create_queue()
        response = self.start_task('task_definition', new_queue_name)
        self.problem_to_instance_dict[problem] = new_queue_name
        return new_queue_name



    # --> If parameter is given check the number of messages in that queue
    # --> If no parameter is given, check the number of messages in the query queue
    def messages_in_queue(self, queue_name=None):
        if queue_name is None:
            return int(self.sqs.get_queue_attributes(QueueUrl=queue_name, QueueAttribute='ApproximateNumberOfMessages'))
        else:
            return int(self.sqs.get_queue_attributes(QueueUrl=self.controller_queue_url, QueueAttribute='ApproximateNumberOfMessages'))