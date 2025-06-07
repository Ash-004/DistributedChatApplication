"use client"

import type React from "react"

import { useState, useEffect, useRef } from "react"
import { type Message, type MessageCreate, type Room, fetchMessages, sendMessage } from "../api/chatApi"
import ChatMessage from "./ChatMessage"
import { v4 as uuidv4 } from "uuid"

interface ChatRoomProps {
  room: Room
  userId: string
}

export default function ChatRoom({ room, userId }: ChatRoomProps) {
  const [messages, setMessages] = useState<Message[]>([])
  const [newMessage, setNewMessage] = useState("")
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [isSending, setIsSending] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (room) {
      setMessages([])
      loadMessages()
      const intervalId = setInterval(loadMessages, 3000)
      return () => clearInterval(intervalId)
    }
  }, [room])

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const loadMessages = async () => {
    if (!room) return

    try {
      setIsLoading(true)
      const messageList = await fetchMessages(room.id)

      setMessages((prevMessages) => {
        const uniqueMessages = new Map()

        prevMessages.forEach((msg) => uniqueMessages.set(msg.id, msg))
        messageList.forEach((msg) => uniqueMessages.set(msg.id, msg))

        return Array.from(uniqueMessages.values()).sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0))
      })
      setError(null)
    } catch (error) {
      console.error(`Failed to load messages for room ${room.id}:`, error)
      setError("Failed to load messages. Please try again.")
    } finally {
      setIsLoading(false)
    }
  }

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!newMessage.trim() || !room) return

    try {
      setIsSending(true)
      const messageData: MessageCreate = {
        room_id: room.id,
        user_id: userId || uuidv4(),
        content: newMessage.trim(),
      }

      await sendMessage(messageData)
      setNewMessage("")
      await loadMessages()
    } catch (error) {
      console.error("Failed to send message:", error)
      setError("Failed to send message. Please try again.")
    } finally {
      setIsSending(false)
    }
  }

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }

  return (
    <div className="flex flex-col h-full relative">
      {/* Room header */}
      <div className="bg-primary-background p-6 shadow-2xl border-b border-white/10 backdrop-blur-2xl">
        <div className="flex items-center">
          <div className="w-16 h-16 rounded-2xl bg-secondary-background flex items-center justify-center text-foreground font-bold text-xl mr-4 shadow-xl">
            {room.name.substring(0, 1).toUpperCase()}
          </div>
          <div>
            <h2 className="font-semibold text-xl text-foreground mb-1">{room.name}</h2>
            <p className="text-secondary-text text-sm">Created by: {room.created_by || "Unknown"}</p>
          </div>
        </div>
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto p-8 scrollbar-thin scrollbar-thumb-white/20 scrollbar-track-transparent relative bg-primary-background">
        {/* Background decoration */}
        <div className="absolute inset-0 pointer-events-none"></div>

        {isLoading && messages.length === 0 ? (
          <div className="flex justify-center items-center h-full">
            <div className="relative">
              <div className="w-16 h-16 border-4 border-white/20 border-t-accent-teal rounded-full animate-spin"></div>
              <div className="absolute inset-0 w-16 h-16 border-4 border-transparent border-t-accent-purple rounded-full animate-spin animate-reverse"></div>
            </div>
          </div>
        ) : messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center animate-fade-in-up">
            <div className="bg-secondary-background rounded-3xl p-16 border border-white/10 max-w-md">
              <div className="relative w-24 h-24 mb-8 mx-auto">
                <div className="absolute inset-0 bg-accent-teal rounded-full animate-pulse-glow"></div>
                <div className="relative bg-accent-purple rounded-full h-full w-full flex items-center justify-center shadow-2xl">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-12 w-12 text-white"
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
              <p className="text-2xl font-bold mb-4 text-foreground">Start the conversation</p>
              <p className="text-secondary-text text-lg">Be the first to share your thoughts!</p>
            </div>
          </div>
        ) : (
          <div className="space-y-6 relative z-10">
            {messages.map((message, index) => (
              <div key={message.id} className="animate-slide-in" style={{ animationDelay: `${index * 50}ms` }}>
                <ChatMessage message={message} isCurrentUser={message.user_id === userId} />
              </div>
            ))}
            <div ref={messagesEndRef} />
          </div>
        )}

        {error && (
          <div className="fixed bottom-24 left-1/2 transform -translate-x-1/2 bg-red-500/20 border border-red-500/30 text-red-200 p-4 rounded-2xl text-sm backdrop-blur-xl shadow-2xl animate-bounce-in z-50">
            {error}
          </div>
        )}
      </div>

      {/* Message input area */}
      <div className="bg-primary-background p-6 border-t border-white/10 backdrop-blur-2xl">
        <form onSubmit={handleSendMessage} className="flex gap-4">
          <input
            type="text"
            value={newMessage}
            onChange={(e) => setNewMessage(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                handleSendMessage(e);
              }
            }}
            placeholder="Type your message..."
            className="flex-1 bg-secondary-background border border-white/10 rounded-2xl px-6 py-4 text-foreground placeholder-secondary-text focus:outline-none focus:ring-2 focus:ring-accent-teal focus:border-transparent transition-all duration-300 text-lg font-medium"
            disabled={isSending}
            tabIndex={0}
          />
          <button
            type="submit"
            disabled={!newMessage.trim() || isSending}
            className="bg-accent-teal hover:bg-accent-purple disabled:bg-gray-700 text-white px-8 py-4 rounded-2xl font-bold transition-all duration-300 transform hover:scale-105 disabled:scale-100 shadow-xl hover:shadow-2xl flex items-center gap-3 text-lg border border-white/20"
            tabIndex={0}
          >
            {isSending ? (
              <>
                <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                Sending
              </>
            ) : (
              <>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-6 w-6"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"
                  />
                </svg>
                Send
              </>
            )}
          </button>
        </form>
      </div>
    </div>
  )
}
