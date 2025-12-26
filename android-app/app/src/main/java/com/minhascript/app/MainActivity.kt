package com.minhascript.app

import android.os.Bundle
import android.view.LayoutInflater
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.isVisible
import androidx.recyclerview.widget.LinearLayoutManager
import com.google.android.material.textfield.TextInputEditText
import com.minhascript.app.databinding.ActivityMainBinding
import com.minhascript.app.databinding.DialogAddMemberBinding

class MainActivity : AppCompatActivity() {
    private lateinit var binding: ActivityMainBinding
    private lateinit var repository: MemberRepository
    private lateinit var adapter: MemberAdapter

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        repository = MemberRepository(this)

        val initialMembers = repository.loadMembers().toMutableList()
        adapter = MemberAdapter(initialMembers) { member ->
            adapter.removeMember(member)
            repository.saveMembers(adapter.currentMembers())
            updateEmptyState()
        }

        binding.membersRecycler.layoutManager = LinearLayoutManager(this)
        binding.membersRecycler.adapter = adapter

        binding.addMemberFab.setOnClickListener { showAddMemberDialog() }

        updateEmptyState()
    }

    private fun showAddMemberDialog() {
        val dialogBinding = DialogAddMemberBinding.inflate(LayoutInflater.from(this))
        val dialog = AlertDialog.Builder(this)
            .setTitle(R.string.add_member_title)
            .setView(dialogBinding.root)
            .setPositiveButton(R.string.add_member_action, null)
            .setNegativeButton(R.string.cancel_action, null)
            .create()

        dialog.setOnShowListener {
            dialog.getButton(AlertDialog.BUTTON_POSITIVE).setOnClickListener {
                val nin = dialogBinding.inputNin.readText()
                val wassit = dialogBinding.inputWassit.readText()
                val ccp = dialogBinding.inputCcp.readText()
                val phone = dialogBinding.inputPhone.readText()

                if (nin.isBlank() || wassit.isBlank() || ccp.isBlank()) {
                    Toast.makeText(this, R.string.add_member_validation, Toast.LENGTH_SHORT).show()
                    return@setOnClickListener
                }

                val member = Member(
                    nin = nin,
                    wassitNo = wassit,
                    ccp = ccp,
                    phoneNumber = phone
                )
                adapter.addMember(member)
                repository.saveMembers(adapter.currentMembers())
                updateEmptyState()
                dialog.dismiss()
            }
        }

        dialog.show()
    }

    private fun updateEmptyState() {
        val isEmpty = adapter.currentMembers().isEmpty()
        binding.emptyState.isVisible = isEmpty
        binding.membersRecycler.isVisible = !isEmpty
    }

    private fun TextInputEditText.readText(): String {
        return text?.toString()?.trim().orEmpty()
    }
}
