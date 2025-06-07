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
    <div className="flex flex-col h-screen bg-primary-background relative">
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-accent-purple rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-blob"></div>
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-accent-teal rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-blob animation-delay-2000"></div>
        <div className="absolute top-40 left-40 w-80 h-80 bg-accent-purple rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-blob animation-delay-4000"></div>
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
            className="md:hidden fixed top-20 left-4 z-40 bg-secondary-background backdrop-blur-xl shadow-2xl rounded-2xl p-3 hover:bg-white/20 transition-all duration-300 border border-white/20 group"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-6 w-6 text-foreground group-hover:scale-110 transition-transform duration-200"
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
                <div className="bg-primary-background rounded-3xl p-16 shadow-2xl border border-white/20 relative overflow-hidden">
                  {/* Background decoration */}
                  <div className="absolute inset-0 bg-transparent"></div>
                  <div className="absolute top-0 right-0 w-32 h-32 bg-accent-teal/20 rounded-full blur-2xl animate-float"></div>
                  <div className="absolute bottom-0 left-0 w-32 h-32 bg-accent-purple/20 rounded-full blur-2xl animate-float-delayed"></div>

                  <div className="relative z-10">
                    {/* Animated logo */}
                    <div className="relative mx-auto w-32 h-32 mb-12">
                      <div className="absolute inset-0 bg-accent-teal rounded-full animate-pulse-glow"></div>
                      <div className="relative bg-accent-purple rounded-full h-full w-full flex items-center justify-center shadow-2xl transform hover:scale-105 transition-transform duration-300">
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

                    <h2 className="text-4xl font-bold text-foreground mb-6 leading-tight">
                      Welcome to Distributed Chat
                    </h2>

                    <p className="text-secondary-text mb-12 text-xl leading-relaxed font-light">
                      Experience next-generation real-time communication powered by distributed consensus algorithms
                    </p>

                    <div className="bg-secondary-background rounded-2xl p-8 border border-white/10 mb-12">
                      <div className="flex items-center justify-center mb-4">
                        <div className="w-12 h-12 bg-accent-teal rounded-2xl flex items-center justify-center mr-4 shadow-lg">
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
                        <h3 className="font-bold text-foreground text-xl">Raft Consensus Protocol</h3>
                      </div>
                      <p className="text-secondary-text leading-relaxed">
                        Built with fault-tolerant distributed systems architecture ensuring message consistency and
                        reliability across multiple nodes, even in challenging network conditions.
                      </p>
                    </div>

                    {/* Enhanced feature grid */}
                    <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
                      <div className="bg-secondary-background p-6 rounded-2xl border border-white/10 hover:border-white/20 transition-all duration-300 group hover:scale-105">
                        <div className="w-16 h-16 bg-accent-teal rounded-2xl flex items-center justify-center mx-auto mb-4 shadow-lg group-hover:shadow-xl transition-shadow duration-300">
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
                        <h4 className="font-bold text-foreground mb-2 text-lg">Lightning Fast</h4>
                        <p className="text-secondary-text text-sm leading-relaxed">
                          Real-time messaging with sub-second latency
                        </p>
                      </div>

                      <div className="bg-secondary-background p-6 rounded-2xl border border-white/10 hover:border-white/20 transition-all duration-300 group hover:scale-105">
                        <div className="w-16 h-16 bg-accent-purple rounded-2xl flex items-center justify-center mx-auto mb-4 shadow-lg group-hover:shadow-xl transition-shadow duration-300">
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
                        <h4 className="font-bold text-foreground mb-2 text-lg">Ultra Secure</h4>
                        <p className="text-secondary-text text-sm leading-relaxed">End-to-end encrypted communication</p>
                      </div>

                      <div className="bg-secondary-background p-6 rounded-2xl border border-white/10 hover:border-white/20 transition-all duration-300 group hover:scale-105">
                        <div className="w-16 h-16 bg-accent-teal rounded-2xl flex items-center justify-center mx-auto mb-4 shadow-lg group-hover:shadow-xl transition-shadow duration-300">
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
                              d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4"
                            />
                          </svg>
                        </div>
                        <h4 className="font-bold text-foreground mb-2 text-lg">Highly Scalable</h4>
                        <p className="text-secondary-text text-sm leading-relaxed">
                          Seamlessly handles thousands of concurrent users
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
