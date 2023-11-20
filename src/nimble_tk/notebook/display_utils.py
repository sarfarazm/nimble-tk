def format_spoken(num, round_num=False):
    sign = 1
    if num < 0:
        sign = -1
        num = num * -1

    sign_char = ''
    if sign < 1:
        sign_char = '-'

    if num > 999999999:
        if round_num:
            return f'{sign_char}{rund(num / 1000000000)} billion'
        return f'{sign_char}{int(num / 1000000000)}.{int( (num % 1000000000)/1000000 ):02d} billion'
    elif num > 999999:
        if round_num:
            return f'{sign_char}{rund(num / 1000000)} billion'
        return f'{sign_char}{int(num / 1000000)}.{int( (num % 1000000)/1000 ):02d} million'
    else:
        return f'{sign_char}{num}'
    
def format_spoken_indian(num, round_num=False):
    sign = 1
    if num < 0:
        sign = -1
        num = num * -1

    sign_char = ''
    if sign < 1:
        sign_char = '-'

    if num > 9999999:
        if round_num:
            return f'{sign_char}{int( round(num / 10000000) )} crores'
        return f'{sign_char}{int(num / 10000000)}.{int( (num % 10000000)/100000 ):02d} crores'
    elif num > 99999:
        if round_num:
            return f'{sign_char}{int( round(num / 100000) )} lacs'
        return f'{sign_char}{int(num / 100000)}.{int( (num % 100000)/1000 ):02d} lacs'
    elif num > 999:
        if round_num:
            return f'{sign_char}{int( round(num / 1000) )} thousand'
        return f'{sign_char}{int(num / 1000)}.{int( (num % 1000)/10):02d} thousand'
    else:
        return f'{sign_char}{num}'
