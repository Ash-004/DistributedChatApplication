"use client"

import { useState, useEffect } from "react"
import { type Room, fetchRooms } from "../api/chatApi"
import CreateRoomModal from "./CreateRoomModal"

interface RoomListProps {
  selectedRoom: Room | null
  onSelectRoom: (room: Room) => void
}

export default function RoomList({ selectedRoom, onSelectRoom }: RoomListProps) {
  const [rooms, setRooms] = useState<Room[]>([])
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    loadRooms()
  }, [])

  const loadRooms = async () => {
    setIsLoading(true)
    setError(null)
    try {
      const roomList = await fetchRooms()
      setRooms(roomList)

      if (roomList.length > 0 && !selectedRoom) {
        onSelectRoom(roomList[0])
      }
    } catch (error) {
      console.error("Failed to load rooms:", error)
      setError("Failed to load rooms. Please check your connection.")
    } finally {
      setIsLoading(false)
    }
  }

  const handleRoomCreated = (newRoom: Room) => {
    setRooms([...rooms, newRoom])
    onSelectRoom(newRoom)
  }

  return (
    <div className="w-full h-full glass-premium border-r border-white/10 flex flex-col shadow-2xl backdrop-blur-2xl">
      <div className="p-6 border-b border-white/10 bg-gradient-to-br from-white/5 to-transparent">
        <h2 className="font-bold text-white mb-6 text-2xl">Chat Rooms</h2>

        {error && (
          <div className="bg-red-500/20 border border-red-500/30 text-red-200 p-4 rounded-2xl mb-6 text-sm backdrop-blur-sm">
            {error}
            <button
              className="ml-3 text-red-300 hover:text-red-100 underline font-medium transition-colors duration-200"
              onClick={loadRooms}
            >
              Retry
            </button>
          </div>
        )}

        <button
          onClick={() => setIsModalOpen(true)}
          className="w-full bg-gradient-to-r from-cyan-500 to-purple-600 hover:from-cyan-400 hover:to-purple-500 text-white py-4 px-6 rounded-2xl shadow-xl transition-all duration-300 transform hover:scale-105 hover:shadow-2xl flex items-center justify-center gap-3 font-semibold text-lg border border-white/20"
        >
          <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" viewBox="0 0 20 20" fill="currentColor">
            <path
              fillRule="evenodd"
              d="M10 3a1 1 0 011 1v5h5a1 1 0 110 2h-5v5a1 1 0 11-2 0v-5H4a1 1 0 110-2h5V4a1 1 0 011-1z"
              clipRule="evenodd"
            />
          </svg>
          Create New Room
        </button>
      </div>

      <div className="overflow-y-auto flex-1 scrollbar-thin scrollbar-thumb-white/20 scrollbar-track-transparent">
        {isLoading ? (
          <div className="flex justify-center items-center h-40">
            <div className="relative">
              <div className="w-12 h-12 border-4 border-white/20 border-t-cyan-400 rounded-full animate-spin"></div>
              <div className="absolute inset-0 w-12 h-12 border-4 border-transparent border-t-purple-400 rounded-full animate-spin animate-reverse"></div>
            </div>
          </div>
        ) : rooms.length === 0 ? (
          <div className="p-8 text-center text-white/70 animate-fade-in">
            <div className="glass-card rounded-3xl p-12 border border-white/10">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-16 w-16 mx-auto text-white/40 mb-6"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M17 8h2a2 2 0 012 2v6a2 2 0 01-2 2h-2v4l-4-4H9a1.994 1.994 0 01-1.414-.586m0 0L11 14h4a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2v4l.586-.586z"
                />
              </svg>
              <p className="text-xl mb-3 font-semibold text-white">No rooms available</p>
              <p className="text-white/60">Create your first room to get started</p>
            </div>
          </div>
        ) : (
          <div className="p-4 space-y-3">
            {rooms.map((room, index) => (
              <div key={room.id} className="animate-slide-in" style={{ animationDelay: `${index * 100}ms` }}>
                <button
                  onClick={() => onSelectRoom(room)}
                  className={`w-full text-left p-5 rounded-2xl transition-all duration-300 transform hover:scale-105 border ${
                    selectedRoom?.id === room.id
                      ? "bg-gradient-to-r from-cyan-500/20 to-purple-500/20 border-cyan-400/50 shadow-xl shadow-cyan-500/20 text-white"
                      : "glass-card border-white/10 text-white/90 hover:border-white/30 hover:bg-white/10"
                  }`}
                >
                  <div className="flex items-center">
                    <div
                      className={`rounded-2xl h-14 w-14 flex items-center justify-center font-bold mr-4 text-lg shadow-lg ${
                        selectedRoom?.id === room.id
                          ? "bg-gradient-to-r from-cyan-400 to-purple-500 text-white"
                          : "bg-gradient-to-r from-white/20 to-white/10 text-white border border-white/20"
                      }`}
                    >
                      {room.name ? room.name.substring(0, 1).toUpperCase() : "?"}
                    </div>
                    <div className="flex-1">
                      <div
                        className={`font-bold text-lg mb-1 ${selectedRoom?.id === room.id ? "text-white" : "text-white/90"}`}
                      >
                        {room.name || "Unnamed Room"}
                      </div>
                      <div className={`text-sm ${selectedRoom?.id === room.id ? "text-cyan-200" : "text-white/60"}`}>
                        Created by: {room.created_by || "Unknown"}
                      </div>
                    </div>
                    {selectedRoom?.id === room.id && (
                      <div className="w-3 h-3 bg-cyan-400 rounded-full animate-pulse shadow-lg"></div>
                    )}
                  </div>
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <CreateRoomModal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} onRoomCreated={handleRoomCreated} />
    </div>
  )
}
