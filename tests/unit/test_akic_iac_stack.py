import aws_cdk as core
import aws_cdk.assertions as assertions

from akic_iac.akic_ia_c_stack import AkicIaCStack

# example tests. To run these tests, uncomment this file along with the example
# resource in akic_ia_c/akic_ia_c_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AkicIaCStack(app, "akic-ia-c")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
