"use client"

import type React from "react"

import { useState, useRef, useEffect } from "react"
import { type Room, type RoomCreate, createRoom } from "../api/chatApi"

interface CreateRoomModalProps {
  isOpen: boolean
  onClose: () => void
  onRoomCreated: (newRoom: Room) => void
}

export default function CreateRoomModal({ isOpen, onClose, onRoomCreated }: CreateRoomModalProps) {
  const [roomName, setRoomName] = useState("")
  const [isCreating, setIsCreating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const modalRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (isOpen && inputRef.current) {
      setTimeout(() => {
        inputRef.current?.focus()
      }, 100)
    }
  }, [isOpen])

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (modalRef.current && !modalRef.current.contains(event.target as Node)) {
        onClose()
      }
    }

    if (isOpen) {
      document.addEventListener("mousedown", handleClickOutside)
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside)
    }
  }, [isOpen, onClose])

  useEffect(() => {
    const handleEscKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose()
      }
    }

    if (isOpen) {
      document.addEventListener("keydown", handleEscKey)
    }

    return () => {
      document.removeEventListener("keydown", handleEscKey)
    }
  }, [isOpen, onClose])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!roomName.trim()) {
      setError("Room name cannot be empty")
      return
    }

    try {
      setIsCreating(true)
      setError(null)
      const roomData: RoomCreate = { name: roomName.trim() }
      const newRoom = await createRoom(roomData)
      setRoomName("")
      onRoomCreated(newRoom)
      onClose()
    } catch (error) {
      console.error("Failed to create room:", error)
      setError("Failed to create room. Please try again.")
    } finally {
      setIsCreating(false)
    }
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div
        ref={modalRef}
        className="bg-primary-background rounded-3xl shadow-2xl w-full max-w-md mx-4 overflow-hidden transform transition-all border border-white/20"
      >
        <div className="bg-secondary-background px-8 py-6">
          <h3 className="text-xl font-bold text-foreground">Create New Room</h3>
          <p className="text-secondary-text mt-2">Start a new conversation space</p>
        </div>

        <form onSubmit={handleSubmit} className="p-8">
          {error && (
            <div className="bg-red-500/20 border border-red-500/30 text-red-200 p-4 rounded-2xl mb-6 text-sm backdrop-blur-sm">
              {error}
            </div>
          )}

          <div className="mb-8">
            <label htmlFor="roomName" className="block text-lg font-semibold text-foreground mb-3">
              Room Name
            </label>
            <input
              ref={inputRef}
              id="roomName"
              type="text"
              value={roomName}
              onChange={(e) => setRoomName(e.target.value)}
              placeholder="Enter room name"
              className="w-full p-4 border border-white/20 rounded-2xl focus:ring-2 focus:ring-accent-teal focus:border-transparent transition-all bg-white/10 text-foreground placeholder-secondary-text backdrop-blur-sm text-lg font-medium"
              disabled={isCreating}
            />
          </div>

          <div className="flex justify-center gap-4">
            <button
              type="button"
              onClick={onClose}
              className="px-8 py-4 border border-white/20 rounded-2xl text-foreground hover:bg-white/10 transition-all duration-300 font-semibold backdrop-blur-sm"
              disabled={isCreating}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={!roomName.trim() || isCreating}
              className="px-8 py-4 bg-accent-teal hover:bg-accent-purple text-white rounded-2xl transition-all duration-300 disabled:opacity-50 flex items-center font-semibold shadow-xl hover:shadow-2xl transform hover:scale-105 disabled:scale-100"
            >
              {isCreating ? (
                <>
                  <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin mr-3"></div>
                  Creating...
                </>
              ) : (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-5 w-5 mr-3"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path
                      fillRule="evenodd"
                      d="M10 3a1 1 0 011 1v5h5a1 1 0 110 2h-5v5a1 1 0 11-2 0v-5H4a1 1 0 110-2h5V4a1 1 0 011-1z"
                      clipRule="evenodd"
                    />
                  </svg>
                  Create Room
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
