import sys

def colorize(s,color):

    #if not sys.stdout.isatty():
    #    return s

    colorcode = {'red':31,'green':32,'yellow':33,
                'blue':34,'magenta':35,'cyan':36,
                'white':37}

    return '\033[1;%im%s\033[0m' % (colorcode[color],s)

def indent(s,width=4):
    """
    Apply a soft indent to every line in s
    """
    out = []
    for line in s.split('\n'):
        out.append('%s%s'%(' '*width,line))
    return ''.join(out)

def rpad(s,width):
    """
    Put s at leftmost of a string of length width, padding any missing
    characters on the right with spaces. If s is longer than width, truncate,
    then replace the last three characters with ellipses. Always returns a
    string of length width.

    Not tty escape sequence aware--call before colorizing
    """

    if len(s) > width:
        return '%s...' % (s[0:width-3])

    return '%s%s' % (s,' ' * (width-len(s)))

def cpad(s,width):
    """
    center s within a string of length width
    """
    p = width-len(s)
    if p <= 0:
        return rpad(s,width)

    rp = p/2
    lp = p/2
    if lp+rp != width:
        rp += 1

    return '%s%s%s'%(' '*lp,s,' '*rp)
