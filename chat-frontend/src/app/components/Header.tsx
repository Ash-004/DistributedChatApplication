"use client"

export default function Header() {
  return (
    <header className="bg-primary-background border-b border-white/10 relative z-20">
      <div className="relative px-8 py-6">
        <div className="flex items-center justify-between">
          {/* Logo and title */}
          <div className="flex items-center space-x-6">
            <div className="relative group">
              <div className="relative bg-accent-teal p-4 rounded-2xl shadow-xl transform group-hover:scale-105 transition-all duration-300">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-10 w-10 text-white"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"
                  />
                </svg>
              </div>
            </div>

            <div>
              <h1 className="text-foreground text-xl font-bold">
                Distributed Chat
              </h1>
              <p className="text-secondary-text text-sm mt-1">Powered by Raft Consensus Protocol</p>
            </div>
          </div>

          {/* Status indicators */}
          <div className="flex items-center space-x-4">
            {/* Network status */}
            <div className="hidden sm:flex items-center space-x-3 bg-secondary-background rounded-2xl px-6 py-3 border border-white/10 hover:bg-secondary-background/70 transition-all duration-300">
              <div className="relative">
                <div className="w-3 h-3 bg-emerald-400 rounded-full shadow-lg"></div>
                <div className="absolute inset-0 w-3 h-3 bg-emerald-400 rounded-full animate-ping opacity-75"></div>
              </div>
              <span className="text-foreground font-semibold">Online</span>
            </div>

            {/* Node count indicator */}
            <div className="hidden md:flex items-center space-x-3 bg-secondary-background rounded-2xl px-6 py-3 border border-white/10 hover:bg-secondary-background/70 transition-all duration-300">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-5 w-5 text-cyan-300"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
                />
              </svg>
              <span className="text-foreground font-semibold">3 Nodes</span>
            </div>

            {/* Settings button */}
            <button
              className="p-4 rounded-2xl bg-secondary-background hover:bg-accent-teal transition-all duration-300 group border border-white/10 hover:border-accent-teal shadow-xl hover:shadow-2xl"
              tabIndex={0}
              aria-label="Settings"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6 text-white/90 group-hover:text-white group-hover:rotate-90 transition-all duration-500"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"
                />
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"
                />
              </svg>
            </button>
          </div>
        </div>
      </div>
    </header>
  )
}
