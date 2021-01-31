import core.handler as handler


def lambda_handler(event, context):

    return handler.LAMBDA_HANDLER.run(event=event, context=context)
