"use client"
import { useState, useEffect } from "react"
import Header from "./components/Header"
import RoomList from "./components/RoomList"
import ChatRoom from "./components/ChatRoom"
import type { Room } from "./api/chatApi"

export default function Home() {
  const [selectedRoom, setSelectedRoom] = useState<Room | null>(null)
  const [userId, setUserId] = useState<string>("")
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  useEffect(() => {
    if (!userId) {
      const newUserId = `user_${Math.random().toString(36).substring(2, 8)}`
      setUserId(newUserId)
    }
  }, [])

  const handleSelectRoom = (room: Room) => {
    setSelectedRoom(room)
    setIsMobileMenuOpen(false)
  }

  return (
    <div className="flex flex-col h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 relative overflow-hidden">
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-purple-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-blob"></div>
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-cyan-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-blob animation-delay-2000"></div>
        <div className="absolute top-40 left-40 w-80 h-80 bg-pink-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-blob animation-delay-4000"></div>
      </div>

      <Header />

      <div className="flex flex-1 overflow-hidden relative z-10">
        {/* Mobile overlay */}
        {isMobileMenuOpen && (
          <div
            className="fixed inset-0 bg-black/60 backdrop-blur-sm z-40 md:hidden transition-all duration-300"
            onClick={() => setIsMobileMenuOpen(false)}
          />
        )}

        {/* Sidebar */}
        <div
          className={`
          ${isMobileMenuOpen ? "translate-x-0" : "-translate-x-full"}
          md:translate-x-0 fixed md:relative z-50 md:z-auto
          transition-all duration-500 ease-out
          w-80 md:w-80 h-full
        `}
        >
          <RoomList selectedRoom={selectedRoom} onSelectRoom={handleSelectRoom} />
        </div>

        {/* Main chat area */}
        <div className="flex-1 flex flex-col min-w-0 relative">
          {/* Mobile menu button */}
          <button
            onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
            className="md:hidden fixed top-20 left-4 z-40 bg-white/10 backdrop-blur-xl shadow-2xl rounded-2xl p-3 hover:bg-white/20 transition-all duration-300 border border-white/20 group"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-6 w-6 text-white group-hover:scale-110 transition-transform duration-200"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>

          {selectedRoom ? (
            <ChatRoom room={selectedRoom} userId={userId} />
          ) : (
            <div className="flex items-center justify-center h-full p-8">
              <div className="text-center max-w-2xl animate-fade-in-up">
                <div className="glass-premium rounded-3xl p-16 shadow-2xl border border-white/20 relative overflow-hidden">
                  {/* Background decoration */}
                  <div className="absolute inset-0 bg-gradient-to-br from-white/5 to-transparent"></div>
                  <div className="absolute top-0 right-0 w-32 h-32 bg-gradient-to-br from-cyan-400/20 to-transparent rounded-full blur-2xl"></div>
                  <div className="absolute bottom-0 left-0 w-32 h-32 bg-gradient-to-tr from-purple-400/20 to-transparent rounded-full blur-2xl"></div>

                  <div className="relative z-10">
                    {/* Animated logo */}
                    <div className="relative mx-auto w-32 h-32 mb-12">
                      <div className="absolute inset-0 bg-gradient-to-r from-cyan-400 to-purple-500 rounded-full animate-pulse-glow"></div>
                      <div className="relative bg-gradient-to-r from-cyan-500 to-purple-600 rounded-full h-full w-full flex items-center justify-center shadow-2xl transform hover:scale-105 transition-transform duration-300">
                        <svg
                          xmlns="http://www.w3.org/2000/svg"
                          className="h-16 w-16 text-white"
                          fill="none"
                          viewBox="0 0 24 24"
                          stroke="currentColor"
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={1.5}
                            d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"
                          />
                        </svg>
                      </div>
                    </div>

                    <h2 className="text-4xl font-bold bg-gradient-to-r from-white via-cyan-200 to-purple-200 bg-clip-text text-transparent mb-6 leading-tight">
                      Welcome to Distributed Chat
                    </h2>

                    <p className="text-white/80 mb-12 text-xl leading-relaxed font-light">
                      Experience next-generation real-time communication powered by distributed consensus algorithms
                    </p>

                    <div className="glass-card rounded-2xl p-8 border border-white/10 mb-12 bg-gradient-to-br from-white/5 to-white/10">
                      <div className="flex items-center justify-center mb-4">
                        <div className="w-12 h-12 bg-gradient-to-r from-emerald-400 to-cyan-500 rounded-2xl flex items-center justify-center mr-4 shadow-lg">
                          <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-6 w-6 text-white"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                          >
                            <path
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              strokeWidth={2}
                              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                            />
                          </svg>
                        </div>
                        <h3 className="font-bold text-white text-xl">Raft Consensus Protocol</h3>
                      </div>
                      <p className="text-white/70 leading-relaxed">
                        Built with fault-tolerant distributed systems architecture ensuring message consistency and
                        reliability across multiple nodes, even in challenging network conditions.
                      </p>
                    </div>

                    {/* Enhanced feature grid */}
                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
                      <div className="glass-card p-6 rounded-2xl border border-white/10 hover:border-white/20 transition-all duration-300 group hover:scale-105">
                        <div className="w-16 h-16 bg-gradient-to-br from-emerald-400 to-emerald-600 rounded-2xl flex items-center justify-center mx-auto mb-4 shadow-lg group-hover:shadow-xl transition-shadow duration-300">
                          <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-8 w-8 text-white"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                          >
                            <path
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              strokeWidth={2}
                              d="M13 10V3L4 14h7v7l9-11h-7z"
                            />
                          </svg>
                        </div>
                        <h4 className="font-bold text-white mb-2 text-lg">Lightning Fast</h4>
                        <p className="text-white/70 text-sm leading-relaxed">
                          Real-time messaging with sub-second latency
                        </p>
                      </div>

                      <div className="glass-card p-6 rounded-2xl border border-white/10 hover:border-white/20 transition-all duration-300 group hover:scale-105">
                        <div className="w-16 h-16 bg-gradient-to-br from-purple-400 to-purple-600 rounded-2xl flex items-center justify-center mx-auto mb-4 shadow-lg group-hover:shadow-xl transition-shadow duration-300">
                          <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-8 w-8 text-white"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                          >
                            <path
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              strokeWidth={2}
                              d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
                            />
                          </svg>
                        </div>
                        <h4 className="font-bold text-white mb-2 text-lg">Ultra Secure</h4>
                        <p className="text-white/70 text-sm leading-relaxed">End-to-end encrypted communication</p>
                      </div>

                      <div className="glass-card p-6 rounded-2xl border border-white/10 hover:border-white/20 transition-all duration-300 group hover:scale-105">
                        <div className="w-16 h-16 bg-gradient-to-br from-cyan-400 to-cyan-600 rounded-2xl flex items-center justify-center mx-auto mb-4 shadow-lg group-hover:shadow-xl transition-shadow duration-300">
                          <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-8 w-8 text-white"
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
                        </div>
                        <h4 className="font-bold text-white mb-2 text-lg">Distributed</h4>
                        <p className="text-white/70 text-sm leading-relaxed">Fault-tolerant multi-node architecture</p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Enhanced footer */}
      <footer className="glass-premium border-t border-white/10 backdrop-blur-2xl relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent"></div>
        <div className="relative px-6 py-6">
          <div className="flex flex-col sm:flex-row justify-between items-center gap-4">
            <div className="flex items-center">
              <div className="relative mr-4">
                <div className="w-4 h-4 bg-emerald-400 rounded-full animate-pulse shadow-lg"></div>
                <div className="absolute inset-0 w-4 h-4 bg-emerald-400 rounded-full animate-ping opacity-75"></div>
              </div>
              <span className="text-white/90 font-medium text-lg">
                Connected as: <span className="text-cyan-300 font-bold">{userId}</span>
              </span>
            </div>

            <div className="flex items-center gap-6 text-white/70">
              <div className="flex items-center gap-3 px-4 py-2 rounded-full bg-white/10 backdrop-blur-sm border border-white/20">
                <div className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse"></div>
                <span className="font-medium">Raft Consensus</span>
              </div>
              <div className="w-px h-6 bg-white/20"></div>
              <div className="flex items-center gap-3 px-4 py-2 rounded-full bg-white/10 backdrop-blur-sm border border-white/20">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-4 w-4 text-cyan-400"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
                <span className="font-medium">Real-time</span>
              </div>
            </div>
          </div>
        </div>
      </footer>
    </div>
  )
}
