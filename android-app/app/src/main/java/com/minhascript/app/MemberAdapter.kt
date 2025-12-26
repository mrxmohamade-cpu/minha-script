package com.minhascript.app

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import com.minhascript.app.databinding.ItemMemberBinding

class MemberAdapter(
    private val items: MutableList<Member>,
    private val onDelete: (Member) -> Unit
) : RecyclerView.Adapter<MemberAdapter.MemberViewHolder>() {

    class MemberViewHolder(private val binding: ItemMemberBinding) : RecyclerView.ViewHolder(binding.root) {
        fun bind(member: Member, onDelete: (Member) -> Unit) {
            binding.memberName.text = member.nin
            binding.memberDetails.text =
                "رقم وسيط: ${member.wassitNo} • CCP: ${member.ccp} • هاتف: ${member.phoneNumber}"
            binding.memberStatus.text = member.status
            binding.deleteButton.setOnClickListener { onDelete(member) }
        }
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): MemberViewHolder {
        val inflater = LayoutInflater.from(parent.context)
        val binding = ItemMemberBinding.inflate(inflater, parent, false)
        return MemberViewHolder(binding)
    }

    override fun getItemCount(): Int = items.size

    override fun onBindViewHolder(holder: MemberViewHolder, position: Int) {
        holder.bind(items[position], onDelete)
    }

    fun addMember(member: Member) {
        items.add(0, member)
        notifyItemInserted(0)
    }

    fun removeMember(member: Member) {
        val index = items.indexOfFirst { it.nin == member.nin }
        if (index != -1) {
            items.removeAt(index)
            notifyItemRemoved(index)
        }
    }

    fun currentMembers(): List<Member> = items.toList()
}
