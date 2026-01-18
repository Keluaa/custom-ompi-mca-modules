#ifndef WAIT_FOR_DEBUGGER_H
#define WAIT_FOR_DEBUGGER_H

#include <cstdio>
#include <csignal>

/**
 * Utility to pause the program indefinitely until a debugger is attached.
 *
 * The 'continue' command in GDB or LLDB will make the program resume.
 * In CLion, the "resume program" button does exactly the same.
 *
 * This is much better than the `int i=0; while (i == 0) sleep(1);` alternative,
 * as there is no variable to edit to resume the program.
 */
inline void wait_for_debugger()
{
    char* wait_for_debugger_env = getenv("WAIT_FOR_DEBUGGER");
    if (wait_for_debugger_env == nullptr || std::string_view(wait_for_debugger_env) != "TRUE") {
        return;
    }
    puts("waiting for debugger");
    raise(SIGSTOP);
}

#endif //WAIT_FOR_DEBUGGER_H