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
    <>
      <div className="w-full h-full bg-background-secondary border-r border-background-secondary flex flex-col">
        <div className="p-6 border-b border-background-secondary">
          <h2 className="font-bold text-foreground mb-6 text-lg">Chat Rooms</h2>

          {error && (
            <div className="bg-red-500/20 border border-red-500/30 text-red-200 p-4 rounded-lg mb-6 text-sm">
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
            className="w-full bg-accent-teal hover:bg-accent-teal/80 text-white py-3 px-4 rounded-xl shadow-lg transition-all duration-300 transform hover:scale-[1.02] flex items-center justify-center gap-2 font-semibold text-base"
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

        <div className="overflow-y-auto flex-1 custom-scrollbar">
          {isLoading ? (
            <div className="flex justify-center items-center h-40">
              <div className="relative">
                <div className="w-12 h-12 border-4 border-white/20 border-t-cyan-400 rounded-full animate-spin"></div>
                <div className="absolute inset-0 w-12 h-12 border-4 border-transparent border-t-purple-400 rounded-full animate-spin animate-reverse"></div>
              </div>
            </div>
          ) : rooms.length === 0 ? (
            <div className="p-8 text-center text-white/70 animate-fade-in">
              <div className="bg-background-primary rounded-xl p-8 border border-white/10">
                <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-12 w-12 mx-auto text-secondary-text mb-4"
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
                <p className="text-lg mb-2 font-semibold text-foreground">No rooms available</p>
                <p className="text-secondary-text">Create your first room to get started</p>
              </div>
            </div>
          ) : (
            <div className="p-4 space-y-2">
              {rooms.map((room, index) => (
                <div key={room.id} className="animate-slide-in" style={{ animationDelay: `${index * 50}ms` }}>
                  <button
                    onClick={() => onSelectRoom(room)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ')
                        onSelectRoom(room);
                    }}
                    tabIndex={0}
                    className={`w-full text-left py-3 px-4 rounded-xl transition-all duration-200 border-l-4 focus:outline-none focus:ring-2 focus:ring-accent-teal focus:ring-offset-2 focus:ring-offset-background-secondary \
                      ${selectedRoom?.id === room.id
                        ? "bg-primary-background border-accent-teal text-foreground font-semibold"
                        : "bg-secondary-background border-transparent text-secondary-background hover:bg-primary-background hover:border-accent-teal/50 hover:text-foreground"}
                    `}
                  >
                    <div className="flex items-center">
                      <div
                        className={`rounded-md h-10 w-10 flex items-center justify-center font-bold mr-3 text-base shadow-sm \
                          ${selectedRoom?.id === room.id
                            ? "bg-accent-teal text-white"
                            : "bg-primary-background text-foreground"}
                        `}
                      >
                        {room.name ? room.name.substring(0, 1).toUpperCase() : "?"}
                      </div>
                      <div className="flex-1">
                        <div
                          className={`text-base \
                            ${selectedRoom?.id === room.id ? "font-semibold text-foreground" : "font-normal text-foreground"}`}
                        >
                          {room.name || "Unnamed Room"}
                        </div>
                        <div className={`text-xs \
                          ${selectedRoom?.id === room.id ? "text-secondary-text" : "text-secondary-text"}`}>
                          Created by: {room.created_by || "Unknown"}
                        </div>
                      </div>

                    </div>
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
      <CreateRoomModal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} onRoomCreated={handleRoomCreated} />
    </>
  )
}
