from VASSAR_controller.aws_interface.aws_interface import AWS_Interface

import sys



# --> Fill in this function!!!
# --> This will turn a queue_url into a message body to be passed to the send message function
def queue_url_to_message_body(queue_url):
    return queue_url

# --> Fill in this function!!!
# --> This retrieves certain message attributes
def get_message_attribute(message, param1=None, param2=None, param3=None, param4=None, param5=None, param6=None, param7=None, param8=None, param9=None)
    return message

# --> Fill in this function!!!
# --> Check to see if the message tells the process to terminate
def check_terminate(message):
    # --> Add the params in here requred to terminate a message
    terminate = get_message_attribute(message)
    if terminate:
        return True
    else:
        return False

def main():

    interface = AWS_Interface('queue_url')
    check_messages = True



    # --> We will check for new messages constantly in a loop!!
    while(check_messages):
        if(interface.messages_in_queue() > 0):
            response = interface.receive_message()
            for message in response["Messages"]:
                if check_messages(message):
                    sys.exit()
                problem = get_message_attribute(message)
                interface.delete_message(message['ReceiptHandle'])
                return_queue = interface.get_problem_queue(problem)
                message_body = queue_url_to_message_body(return_queue)
                interface.send_message(message_body)

    return 0


if __name__ == '__main__':
    main()