"use client"

export default function Header() {
  return (
    <header className="glass-premium border-b border-white/10 backdrop-blur-2xl shadow-2xl relative overflow-hidden z-20">
      {/* Animated background gradient */}
      <div className="absolute inset-0 bg-gradient-to-r from-cyan-500/10 via-purple-500/10 to-pink-500/10 animate-gradient-x"></div>

      <div className="relative px-8 py-6">
        <div className="flex items-center justify-between">
          {/* Logo and title */}
          <div className="flex items-center space-x-6">
            <div className="relative group">
              {/* Animated logo background */}
              <div className="absolute inset-0 bg-gradient-to-r from-cyan-400 to-purple-500 rounded-2xl animate-pulse-glow opacity-75 group-hover:opacity-100 transition-opacity duration-300"></div>
              <div className="relative bg-gradient-to-r from-cyan-500 to-purple-600 p-4 rounded-2xl shadow-2xl transform group-hover:scale-105 transition-all duration-300">
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
              <h1 className="text-3xl font-bold bg-gradient-to-r from-white via-cyan-200 to-purple-200 bg-clip-text text-transparent">
                Distributed Chat
              </h1>
              <p className="text-white/80 text-base font-medium mt-1">Powered by Raft Consensus Protocol</p>
            </div>
          </div>

          {/* Status indicators */}
          <div className="flex items-center space-x-4">
            {/* Network status */}
            <div className="hidden sm:flex items-center space-x-3 bg-white/10 rounded-2xl px-6 py-3 backdrop-blur-xl border border-white/20 hover:bg-white/20 transition-all duration-300">
              <div className="relative">
                <div className="w-3 h-3 bg-emerald-400 rounded-full shadow-lg"></div>
                <div className="absolute inset-0 w-3 h-3 bg-emerald-400 rounded-full animate-ping opacity-75"></div>
              </div>
              <span className="text-white font-semibold">Online</span>
            </div>

            {/* Node count indicator */}
            <div className="hidden md:flex items-center space-x-3 bg-white/10 rounded-2xl px-6 py-3 backdrop-blur-xl border border-white/20 hover:bg-white/20 transition-all duration-300">
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
              <span className="text-white font-semibold">3 Nodes</span>
            </div>

            {/* Settings button */}
            <button className="p-4 rounded-2xl bg-white/10 hover:bg-white/20 transition-all duration-300 backdrop-blur-xl group border border-white/20 hover:border-white/30 shadow-lg hover:shadow-xl">
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

        {/* Floating particles effect */}
        <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none">
          <div className="absolute top-4 left-1/4 w-2 h-2 bg-cyan-300 rounded-full animate-float opacity-60"></div>
          <div className="absolute top-8 right-1/3 w-1 h-1 bg-purple-300 rounded-full animate-float-delayed opacity-60"></div>
          <div className="absolute bottom-6 right-1/4 w-2 h-2 bg-pink-300 rounded-full animate-float opacity-60"></div>
          <div className="absolute bottom-4 left-1/3 w-1 h-1 bg-cyan-300 rounded-full animate-float-delayed opacity-60"></div>
        </div>
      </div>
    </header>
  )
}
