import os


# Set TEST_FLAG env variables so lambdas know whether to call lambda_handler()
os.environ["TEST_FLAG"] = "True"
