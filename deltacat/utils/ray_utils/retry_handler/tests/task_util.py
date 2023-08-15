from deltacat.utils.ray_utils.retry_handler.failures.aws_security_token_rate_exceeded_exception import \
    AWSSecurityTokenRateExceededException


def square_num_ray_task(input_num: int) -> int:
    return input_num * input_num

def square_num_ray_task_with_failures(input_num: int) -> int:
    if input_num in [1, 5, 10]:
        raise AWSSecurityTokenRateExceededException()
    return input_num * input_num